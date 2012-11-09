package graphlab.engine

import scala.io.Source
import scala.io._
import scala.collection.mutable.MutableList
import scala.actors.Futures._
import scala.math._
import graphlab.graph._
import graphlab.user._
import graphlab.engine._

class Master[VertexDataType,EdgeDataType] {
  
  type E = Edge[EdgeDataType,VertexDataType]
  type V = Vertex[VertexDataType]
  type S = Shard[VertexDataType,EdgeDataType]

  var shards:Array[S] = null
  var verts:Array[V] = null
  var edges:Array[E] = null

  //number of shards to split the graph into
  val NUM_SHARDS = 256

  //how to split the graph into shards
  //used for both edges and vertices
  //needs to return a value in [0,NUM_SHARDS)
  private def hash(n:Int):Int = n % NUM_SHARDS

  def build_graph(fname:String,parse_input:(String)=>EdgeDataType,init_vertex:(Int)=>VertexDataType) = {

    //read graph in from file
    val split_input = Source.fromFile(fname).mkString.split("\n").map(_.split("\t"))

    var num_verts = 0
    var num_edges = -1

    def make_edge_tuple(l:Array[java.lang.String]) = l.toList match {
      case List(s,d,data) => {
	val ss = s.trim.toInt
	val dd = d.trim.toInt
	num_verts = max(num_verts,ss)
	num_verts = max(num_verts,dd)
	num_edges = num_edges + 1
	(num_edges,ss,dd,parse_input(data.trim))
      }
      case List(s,d) => {
	val ss = s.trim.toInt
	val dd = d.trim.toInt
	num_verts = max(num_verts,ss)
	num_verts = max(num_verts,dd)
	num_edges = num_edges + 1
	(num_edges,ss,dd,parse_input(""))
      }	
      case _ => throw new RuntimeException("malformed input")
    }
	
    //list of all tuples to construct edges
    val edge_protoplasm = split_input.map(make_edge_tuple)

    def make_vertex(id:Int):V = new graphlab.graph.Vertex(id,init_vertex(id))
    
    //all shards will have reference to this
    verts = (0 until (num_verts + 1)).map(make_vertex).toArray

    def make_edge(x:(Int,Int,Int,EdgeDataType)):E = {
      verts(x._2).inc_out
      verts(x._3).inc_in
      new E(x._1,verts(x._2),verts(x._3),x._4)
    }

    //list of all edges
    edges = edge_protoplasm.map(make_edge)

    def make_shard(id:Int):S = new graphlab.engine.Shard(id)

    //create shards
    shards = (0 until NUM_SHARDS).map(make_shard).toArray

    def e_place(e:E):Unit = {
      shards(hash(e.id)).push_edge(e)
    }

    def v_place(v:V):Unit = {
      shards(hash(v.id)).push_vert(v)
    }

    //give shards appropriate edges and verts
    edges.map(e_place)
    verts.map(v_place)

  }

  def map_reduce_edges[GatherType](
    mapFun:(E)=>GatherType,
    sum:(GatherType,GatherType)=>GatherType) = {
    val g = shards.map(_.map_reduce_edges(mapFun,sum)).flatMap(_.apply())
    g.tail.foldLeft(g.head)(sum)
  }

  def map_reduce_verts[GatherType](
    mapFun:(V)=>GatherType,
    sum:(GatherType,GatherType)=>GatherType) = {
    val g = shards.map(_.map_reduce_verts(mapFun,sum)).flatMap(_.apply())
    g.tail.foldLeft(g.head)(sum)
  }

  def transform_edges(mapFun:(E)=>EdgeDataType) = {
    shards.foreach(_.transform_edges(mapFun))
  }

  def transform_verts(mapFun:(V)=>VertexDataType) = {
    shards.foreach(_.transform_verts(mapFun))
  }
  
  def run_gas[GatherType:Manifest](
    gather:(V,E)=>(EdgeDataType,GatherType),
    sum:(GatherType,GatherType)=>GatherType,
    apply:(V,GatherType)=>VertexDataType,
    scatter:(V,E)=>(EdgeDataType,Boolean),
    default_gather:GatherType,
    gatherdir:Dir,scatterdir:Dir) = {

    println("starting GAS")

    var acc:Array[Either[GatherType,GatherType]] = (0 until verts.length).map(_ => Left(default_gather)).toArray

    var signal:Array[Boolean] = (0 until verts.length).map(_ => true).toArray
    
    //count the iterations
    var iter = 0

    val lock:AnyRef = new AnyRef()

    shards.map(_.register_lock(lock))
    shards.map(_.register_signal(signal))

    //run shards until convergence
    while (signal.foldLeft(false)(_||_)) {

      println("starting gas iteration")

      //reset accumulator to default value
      (0 until verts.length).foreach((n) => acc(n) = Left(default_gather))

      /* GATHER PHASE */

      //what to do when done gathering
      def accumulate(x:((V,GatherType),(E,EdgeDataType))):Unit = {
	val (vert,a) = x._1
	val (edge,data) = x._2
	acc(vert.id) match {
	  case Left(_) => acc(vert.id) = Right(a)
	  case Right(n) => acc(vert.id) = Right(sum(n,a))
	}
	edge.data = data
      }

      //actually run gather on all shards
      shards.map(_.run_gather[GatherType](gather,gatherdir,accumulate)).map(_.apply())

      /* APPLY PHASE */

      //what to do when done applying
      def commit(x:(V,VertexDataType)):Unit = {
	x._1.data = x._2
      }

      //actually run apply on all shards
      shards.map(_.run_apply[GatherType](apply,acc,commit)).map(_.apply())
      
      /* SCATTER PHASE */

      //reset signal to all false
      (0 until verts.length).foreach((n) => signal(n) = false)

      //what to do when done scattering
      def publish(x:((E,EdgeDataType),(V,Boolean))):Unit = {
	val (edge,data) = x._1
	val (vert,sig) = x._2
	signal(vert.id) |= sig
	edge.data = data
      }

      //actually run scatter on all shards
      shards.map(_.run_scatter[GatherType](scatter,scatterdir,publish)).map(_.apply())

      //count the number of iterations
      iter += 1

    }    

  }

  def asString:List[String] = {
    var s:List[String] = List()
    s = "Vertices"::s
    def get_vert(v:V) = {
      s = (v.id + " : " + v.data)::s
    }
    verts.map(get_vert)
    s = ""::s
    s = "Edges"::s
    def get_edge(e:E) = {
      s = (e.source.id + "->" + e.target.id + " : " + e.data)::s
    }
    shards.map(_.edges.map(get_edge))
    s.reverse
  }

  def dump_graph():Unit = {
    asString.foreach(println)
  }

}

  
