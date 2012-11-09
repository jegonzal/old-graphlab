package graphlab

import spark.SparkContext
import spark.SparkContext._
import spark.HashPartitioner
import spark.storage.StorageLevel

class FirstPartitioner(val numPartitions: Int = 4) extends spark.Partitioner {
  def getPartition(key: Any): Int = key match {
    case (first: Int, second: Int) => Math.abs(first) % numPartitions
    case _ => 0
  }
  override def equals(other: Any) = other.isInstanceOf[FirstPartitioner]
}

class Vertex[VD](val id: Int, val data: VD);

class Graph[VD: Manifest, ED: Manifest](
  val vertices: spark.RDD[(Int, VD)],
  val edges: spark.RDD[((Int, Int), ED)]) {

  def cache(): Graph[VD, ED] = {
    new Graph(vertices.cache(), edges.cache())
  }

  def nvertices(): Int = {
    vertices.count().toInt
  }

  def nedges(): Int = {
    edges.count().toInt
  }

  def iterateGAS[A: Manifest](
    gather: (Vertex[VD], ED, Vertex[VD]) => (ED, A),
    sum: (A, A) => A,
    default: A,
    apply: (Vertex[VD], A) => VD,
    scatter: (Vertex[VD], ED, Vertex[VD]) => (ED, Boolean),
    niter: Int,
    gather_edges: String = "in",
    scatter_edges: String = "out") = {

    ClosureCleaner.clean(gather)
    ClosureCleaner.clean(sum)
    ClosureCleaner.clean(apply)
    ClosureCleaner.clean(scatter)

    val numprocs = 4;
    val partitioner = new FirstPartitioner(numprocs);
    val hashpartitioner = new HashPartitioner(numprocs)

    
    // ((pid, source), (target, data))
    var part_edges =
      edges.map {
        case ((source, target), data) => {
          val pid = Math.abs((source, target).hashCode()) % numprocs
          ((pid, source), (target, data))
        }
      }.partitionBy(partitioner).cache() //persist(StorageLevel.DISK_ONLY)
    //println("part_edges")  
    //part_edges.collect.foreach(println)

    // distribute vertices
    // ((pid, vid), (data, bool))
    var vreplicas =
      part_edges.flatMap {
        case ((pid, source), (target, _)) => List((source, pid), (target, pid))
      }.distinct(partitioner.numPartitions).join(vertices).map {
        case (vid, (pid, data)) => ((pid, vid), (data, true))
      }.partitionBy(partitioner).cache()
    //println("vreplicas")  
    //vreplicas.collect.foreach(println)

    // (vid, pid)
    val vlocale = vreplicas.map { case ((pid, vid), vdata) => (vid, pid) }
      					   .partitionBy(hashpartitioner).cache()
    //println("vlocale")  
    //vlocale.collect.foreach(println)

    for (i <- 1 to niter) {

      def join_edges_and_vertices(
        vreplicas: spark.RDD[((Int, Int), (VD, Boolean))],
        part_edges: spark.RDD[((Int, Int), (Int, ED))]) = {
        part_edges.join(vreplicas).map {
          case ((pid, source_id), ((target_id, edata), (source_vdata, source_active))) =>
            ((pid, target_id), (source_id, source_vdata, source_active, edata))
        }.join(vreplicas).map {
          case ((pid, target_id), ((source_id, source_vdata, source_active, edata), (target_vdata, target_active))) =>
            ((pid, source_id), (source_id, source_vdata, source_active, edata, target_id, target_vdata, target_active))
        }
      }

      // Begin iteration    
      System.out.println("Begin iteration:" + i)
      /** Gather Phase --------------------------------------------- */
      val gather_join = join_edges_and_vertices(vreplicas, part_edges)
      
        /*
      part_edges.join(vreplicas).map {
        case ((pid, source), ((target, edata), (vdata_source, active_source))) =>
          ((pid, target), (source, edata, vdata_source))
      }.join(vreplicas).map {
        case ((pid, target), ((source, edata, vdata_source), (vdata_target, active))) =>
          ((pid, source), (source, vdata_source, edata, target, vdata_target))
      } */
      //println("gather_join")    
      //gather_join.collect.foreach(println)

      val gather_ = gather
      val sum_ = sum
      val apply_ = apply
      val gather_edges_ = gather_edges
      val default_ = default

      // (vid, accum)
      val accum = gather_join.flatMap {
        case ((pid, source), 
            (source_id, source_vdata, source_active, edata, 
                target_id, target_vdata, target_active))  => {          
          val sourceVertex = new Vertex[VD](source_id, source_vdata)
          val targetVertex = new Vertex[VD](target_id, target_vdata)
          lazy val (_, target_gather) = gather_(sourceVertex, edata, targetVertex)
          lazy val (_, source_gather) = gather_(targetVertex, edata, sourceVertex)
          // compute the gather as needed
          (if(target_active && (gather_edges_ == "in" || gather_edges_ == "both")) 
            List((target_id, target_gather)) else List()) ++
          (if(source_active && (gather_edges_ == "out" || gather_edges_ == "both")) 
            List((source_id, source_gather)) else List())
        }
      }.reduceByKey(sum_)

      //println("All accum")
      //allaccum.foreach(println)

      /** Apply Phase --------------------------------------------- */
      val vsync = vreplicas
        .map {
          case ((pid, vid), (data, active)) => (vid, (data, active))
        }.distinct(numprocs).leftOuterJoin(accum)
        .map {
          case (vid, ((data, true), Some(accum))) =>
            (vid, (apply_(new Vertex[VD](vid, data), accum), true))
          case (vid, ((data, true), None)) => 
              (vid, (apply_(new Vertex[VD](vid, data), default_), true))
          case (vid, ((data, false), _)) => (vid, (data, false))
        }

        vreplicas = vsync.join(vlocale).map {
        case (vid, ((vdata, active), pid)) => ((pid, vid), (vdata, active))
      }.cache()

      /** Scatter Phase ---------------------------------------------*/
      
      vreplicas.take(10).foreach(println)
    }

    // Collapse vreplicas, edges and retuen a new graph
    val vertices_ret = vreplicas.map { case ((pid, vid), vdata) => (vid, vdata) }.distinct(numprocs)
    val edges_ret = part_edges.map { case ((pid, src), (target, edata)) => ((src, target), edata) }
    new Graph(vertices_ret, edges_ret)
  }
}

object Graph {
  def load_graph[ED: Manifest](sc: SparkContext,
    fname: String, edge_parser: String => ED) = {

    val partitioner = new FirstPartitioner()
    
    val edges = sc.textFile(fname).map(
      line => {
        val source :: target :: tail = line.split("\t").toList
        val edata = edge_parser(tail.mkString("\t"))
        ((source.trim.toInt, target.trim.toInt), edata)
      }).partitionBy(partitioner).persist(StorageLevel.DISK_ONLY)

    val vertices = edges.flatMap {
      case ((source, target), _) => List((source, 1), (target, 1))
    }.reduceByKey(_ + _)
    val g = new Graph[Int, ED](vertices, edges)
    println("Loaded graph: #edges: " + g.nedges() + "  #vertices" + g.nvertices());
    g
  }
}

object GraphTest {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[4]", "pagerank")
    val graph = Graph.load_graph(sc, "/Users/jegonzal/Data/google.tsv", x => false)
    val initial_ranks = graph.vertices.map { case (vid, _) => (vid, 1.0F) }
    val graph2 = new Graph(initial_ranks, graph.edges.sample(false, 0.01, 1))
    val graph_ret = graph2.iterateGAS(
      (v1, edata, v2) => (edata, v2.data), // gather
      (a: Float, b: Float) => a + b,	// sum
      0F,
      (v, a: Float) => (0.15 + 0.85 * a).toFloat,	// apply
      (v1, edata, v2) => (edata, false), // scatter
      5).cache()
    println("Computed graph: #edges: " + graph_ret.nedges() + "  #vertices" + graph_ret.nvertices())
    graph_ret.vertices.take(10).foreach(println)
  }
}