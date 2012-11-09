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

  def cache () : Graph[VD, ED] = {
    new Graph (vertices.cache(), edges.cache())
  }
  
  def nvertices() : Int = {
    vertices.count().toInt
  }
  
  def nedges() : Int = {
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

    
    // distribute edges
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
      // Begin iteration    
      System.out.println("Begin iteration:" + i)
      /** Gather Phase --------------------------------------------- */      
      val gather_join = part_edges.join(vreplicas).map {
        case ((pid, source), ((target, edata), (vdata_source, active_source))) =>
          ((pid, target), (source, edata, vdata_source))
      }.join(vreplicas).map {
        case ((pid, target), ((source, edata, vdata_source), (vdata_target, active))) =>
          ((pid, source), (source, vdata_source, edata, target, vdata_target))
      }
      //println("gather_join")    
      //gather_join.collect.foreach(println)

      val gather_ = gather
      val sum_ = sum
      val apply_ = apply 
      val gather_edges_ = gather_edges
      val default_ = default

      // (vid, accum)
      val accum = gather_join.flatMap {
        case ((pid, source), (_, vdata_source, edata, target, vdata_target)) => {
          val sourceVertex = new Vertex[VD](source, vdata_source)
          val targetVertex = new Vertex[VD](target, vdata_target)
          lazy val (_, trg_gather) = gather_(sourceVertex, edata, targetVertex)
          lazy val (_, src_gather) = gather_(targetVertex, edata, sourceVertex)
          gather_edges_ match {
            case "in" => List((target, trg_gather))
            case "out" => List((source, src_gather))
            case "both" => List((target, trg_gather), (source, src_gather))
            case _ => List()
          }
        }
      }.reduceByKey(sum_)
      
      val allaccum = vertices.leftOuterJoin(accum).map {
        case (vid, (vdata, Some(accum))) => (vid, accum)
        case (vid, (vdata, None)) => (vid, default_)
      }
      //println("All accum")
      //allaccum.foreach(println)
      
     
      /** Apply Phase --------------------------------------------- */
       val vsync = vreplicas
        .map {
          case ((pid, vid), (data, active)) => (vid, data)
        }.distinct(numprocs).join(allaccum)
        .map {
          case (vid, (data, accum)) =>
            (vid, apply_(new Vertex[VD](vid, data), accum))
        }
        //println("vsync")
        //vsync.foreach(println)
                  
      vreplicas = vsync.join(vlocale).map {
        case (vid, (vdata, pid)) => ((pid, vid), (vdata, false))
      }.cache()
      
      /** Scatter Phase --------------------------------------------- 
      val scatter_half_join = part_edges.join(vreplicas).map {
        case ((pid, source), ((target, edata), vdata_source)) =>
          ((pid, target), (source, edata, vdata_source))
      }

      val scatter_ = scatter
      val scatter_edges_ = scatter_edges
      val accum1 = vreplicas.join(scatter_half_join).flatMap {
        case ((pid, target), (vdata_target, (source, edata, vdata_source))) => {
          val sourceVertex = new Vertex[VD](source, vdata_source)
          val targetVertex = new Vertex[VD](target, vdata_target)
          val (_, trg_gather) = gather_(sourceVertex, edata, targetVertex)
          val (_, src_gather) = gather_(targetVertex, edata, sourceVertex)
          gather_edges_ match {
            case "in" => List((target, trg_gather))
            case "out" => List((source, src_gather))
            case "both" => List((target, trg_gather), (source, src_gather))
            case _ => List()
          }
        }
      }
     */      
      vreplicas.take(10).foreach(println)
    }

    // Collapse vreplicas, edges and retuen a new graph
    val vertices_ret = vreplicas.map{case ((pid, vid), vdata) => (vid, vdata)}.distinct(numprocs)
    val edges_ret = part_edges.map{case ((pid, src), (target, edata)) => ((src, target), edata)}
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
    val graph = Graph.load_graph(sc, "/Users/haijieg/tmp/google.tsv", x => false)
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