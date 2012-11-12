package graphlab

import spark.SparkContext
import spark.SparkContext._
import spark.HashPartitioner
import spark.storage.StorageLevel

/**
 * Class containing the id and value of a vertx
 */
class Vertex[VD](val id: Int, val data: VD);

/**
 * Class containing both vertices and the edge data associated with an edge
 */
class Edge[VD, ED](val source: Vertex[VD], val target: Vertex[VD],
  val data: ED) {
  def other(vid: Int) = { if (source.id == vid) target else source; }
  def vertex(vid: Int) = { if (source.id == vid) source else target; }
} // end of class edge

/**
 * A partitioner for tuples that only examines the first value in the tuple.
 */
class FirstPartitioner(val numPartitions: Int = 4) extends spark.Partitioner {
  def getPartition(key: Any): Int = key match {
    case (first: Int, second: Int) => Math.abs(first) % numPartitions
    case _ => 0
  }
  override def equals(other: Any) = other.isInstanceOf[FirstPartitioner]
}

/**
 * A Graph RDD that supports computation on graphs.
 */
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

  /**
   * The join edges and vertices function returns a table which joins
   * the vertex and edge data.
   */
  def join_edges_and_vertices(
    vreplicas: spark.RDD[((Int, Int), (VD, Boolean))],
    part_edges: spark.RDD[((Int, Int), (Int, ED))]) = {
    part_edges
      .join(vreplicas).map {
        // Join with the source vertex data and rekey with target vertex
        case ((pid, source_id), ((target_id, edata), (source_vdata, source_active))) =>
          ((pid, target_id), (source_id, source_vdata, source_active, edata))
      }
      .join(vreplicas).map {
        // Join with the target vertex and rekey back to the source vertex 
        case ((pid, target_id), ((source_id, source_vdata, source_active, edata), (target_vdata, target_active))) =>
          ((pid, source_id), (source_id, source_vdata, source_active, edata, target_id, target_vdata, target_active))
      }
      .filter {
        // Drop any edges that do not have active vertices
        case ((pid, _), (source_id, source_vdata, source_active, edata, target_id, target_vdata, target_active)) =>
          source_active || target_active
      }
  } // end of join edges and vertices

  /**
   * Execute the synchronous powergraph abstraction.
   *
   * gather: (center_vid, edge) => (new edge data, accumulator)
   * Todo: Finish commenting
   */
  def iterateGAS[A: Manifest](
    gather: (Int, Edge[VD, ED]) => (ED, A),
    sum: (A, A) => A,
    default: A,
    apply: (Vertex[VD], A) => VD,
    scatter: (Int, Edge[VD, ED]) => (ED, Boolean),
    niter: Int,
    gather_edges: String = "in",
    scatter_edges: String = "out") = {

    ClosureCleaner.clean(gather)
    ClosureCleaner.clean(sum)
    ClosureCleaner.clean(apply)
    ClosureCleaner.clean(scatter)

    val numprocs = 64;
    val partitioner = new FirstPartitioner(numprocs);
    val hashpartitioner = new HashPartitioner(numprocs)

    // Partition the edges over machines.  The part_edges table has the format
    // ((pid, source), (target, data))
    var part_edges =
      edges.map {
        case ((source, target), data) => {
          // val pid = Math.abs((source, target).hashCode()) % numprocs
          val pid = Math.abs(source.hashCode()) % numprocs
          ((pid, source), (target, data))
        }
      }.partitionBy(partitioner).cache()

    // Duplicate the vertices for all machines that depend on them.  
    // ((pid, vid), (data, is_active))
    var vreplicas =
      part_edges.flatMap {
        case ((pid, source), (target, _)) => List((source, pid), (target, pid))
      }.distinct(partitioner.numPartitions).join(vertices).map {
        case (vid, (pid, data)) => ((pid, vid), (data, true))
      }.partitionBy(partitioner).cache()

    // Create a map from vertex id to the partitions that contain that vertex
    // (vid, pid)
    val vlocale = vreplicas.map { case ((pid, vid), vdata) => (vid, pid) }
      .partitionBy(hashpartitioner).cache()

    // Loop until convergence or there are no active vertices
    var iter = 0
    while (iter < niter && vreplicas
      .map({ case ((_, _), (_, active)) => active }).reduce(_ || _)) {
      // Begin iteration    
      System.out.println("Begin iteration:" + iter)
      iter += 1;

      /** Gather Phase --------------------------------------------- */
      val gather_ = gather
      val sum_ = sum
      val apply_ = apply
      val gather_edges_ = gather_edges
      val default_ = default

      // Compute the accumulator for each vertex
      val accum = join_edges_and_vertices(vreplicas, part_edges).flatMap {
        case ((pid, source),
          (source_id, source_vdata, source_active, edata,
            target_id, target_vdata, target_active)) => {
          val sourceVertex = new Vertex[VD](source_id, source_vdata)
          val targetVertex = new Vertex[VD](target_id, target_vdata)
          val edge = new Edge(sourceVertex, targetVertex, edata);
          lazy val (_, target_gather) = gather_(source_id, edge);
          lazy val (_, source_gather) = gather_(target_id, edge);
          // compute the gather as needed
          (if (target_active && (gather_edges_ == "in" || gather_edges_ == "both"))
            List((target_id, target_gather)) else List()) ++
            (if (source_active && (gather_edges_ == "out" || gather_edges_ == "both"))
              List((source_id, source_gather)) else List())
        }
      }.reduceByKey(sum_)

      /** Apply Phase --------------------------------------------- */
      vreplicas = vreplicas
        .map {
          // Remove the pid information
          case ((pid, vid), (data, active)) => (vid, (data, active))
        }
        .distinct(numprocs) // Reduce to only one copy of each vertex
        .leftOuterJoin(accum) // Merge with the gather result
        .map { // Execute the apply if necessary
          case (vid, ((data, true), Some(accum))) =>
            (vid, (apply_(new Vertex[VD](vid, data), accum), true))
          case (vid, ((data, true), None)) =>
            (vid, (apply_(new Vertex[VD](vid, data), default_), true))
          case (vid, ((data, false), _)) => (vid, (data, false))
        }
        .join(vlocale) // Duplicate
        .map { // Reattach the pid information 
          case (vid, ((vdata, active), pid)) => ((pid, vid), (vdata, active))
        }.cache()

      /** Scatter Phase ---------------------------------------------*/
      val scatter_ = scatter
      val scatter_edges_ = scatter_edges
      val active_vertices = join_edges_and_vertices(vreplicas, part_edges)
        .flatMap {
          case ((pid, source),
            (source_id, source_vdata, source_active, edata,
              target_id, target_vdata, target_active)) => {
            val sourceVertex = new Vertex[VD](source_id, source_vdata)
            val targetVertex = new Vertex[VD](target_id, target_vdata)
            val edge = new Edge(sourceVertex, targetVertex, edata);
            lazy val (_, new_active_target) = scatter_(source_id, edge)
            lazy val (_, new_active_source) = scatter_(target_id, edge)
            // compute the gather as needed
            (if (target_active && (scatter_edges_ == "in" || scatter_edges_ == "both"))
              List((source_id, new_active_source)) else List()) ++
              (if (source_active && (scatter_edges_ == "out" || scatter_edges_ == "both"))
                List((target_id, new_active_target)) else List())
          }
        }.reduceByKey(_ | _)

      val replicate_active = vlocale.leftOuterJoin(active_vertices).map {
        case (vid, (pid, Some(active))) => ((pid, vid), active)
        case (vid, (pid, None)) => ((pid, vid), false)
      }.cache()

      vreplicas = vreplicas.join(replicate_active).map {
        case ((pid, vid), ((vdata, old_active), new_active)) =>
          ((pid, vid), (vdata, new_active))
      }.cache()

      vreplicas.take(10).foreach(println)
    }

    // Collapse vreplicas, edges and retuen a new graph
    val vertices_ret = vreplicas.map { case ((pid, vid), vdata) => (vid, vdata) }.distinct(numprocs)
    val edges_ret = part_edges.map { case ((pid, src), (target, edata)) => ((src, target), edata) }
    new Graph(vertices_ret, edges_ret)
  }
} // End of Graph RDD

/**
 * Graph companion object
 */
object Graph {

  /**
   * Load an edge list from file initializing the Graph RDD
   */
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
    val graph = new Graph[Int, ED](vertices, edges)
    
    println("Loaded graph:" +
        "\n\t#edges:    " + graph.nedges() + 
        "\n\t#vertices: " + graph.nvertices())
    graph
  }
} // End of Graph Object

/**
 * Test object for graph class
 */
object GraphTest {

  def test_pagerank(fname: String) {
    val sc = new SparkContext("local[4]", "pagerank")
    val graph = Graph.load_graph(sc, fname, x => false)
    val initial_ranks = graph.vertices.map { case (vid, _) => (vid, 1.0F) }
    val graph2 = new Graph(initial_ranks, graph.edges)
    val graph_ret = graph2.iterateGAS(
      (me_id, edge) => (edge.data, edge.source.data), // gather
      (a: Float, b: Float) => a + b, // sum
      0F,
      (v, a: Float) => (0.15 + 0.85 * a).toFloat, // apply
      (me_id, edge) => (edge.data, false), // scatter
      5).cache()
    println("Computed graph: #edges: " + graph_ret.nedges() + "  #vertices" + graph_ret.nvertices())
    graph_ret.vertices.take(10).foreach(println)
  }

  def test_connected_component(fname: String) {
    val sc = new SparkContext("local[4]", "connected_component")
    val graph = Graph.load_graph(sc, fname, x => false)
    val initial_ranks = graph.vertices.map { case (vid, _) => (vid, vid) }
    val graph2 = new Graph(initial_ranks, graph.edges)
    val niterations = 10;
    val graph_ret = graph2.iterateGAS(
      (me_id, edge) => (edge.data, edge.other(me_id).data), // gather
      (a:Int, b:Int) => Math.min(a,b), // sum
      Integer.MAX_VALUE,
      (v, a:Int) => if(a < Integer.MAX_VALUE) a else v.id, // apply
      (me_id, edge) => 
        (edge.data, edge.other(me_id).data > edge.vertex(me_id).data), // scatter
      niterations).cache()
    println("Computed graph: #edges: " + graph_ret.nedges() + "  #vertices" + graph_ret.nvertices())
    graph_ret.vertices.take(10).foreach(println)
  }

  def main(args: Array[String]) {
    val fname = if (args.length == 1) args(0)
    else "/Users/jegonzal/Data/google.tsv"
    test_pagerank(fname)
  }
} // end of GraphTest




