package graphlab.engine

import scala.io.Source
import scala.io._
import scala.collection.mutable.MutableList
import scala.actors.Futures._
import scala.math._
import graphlab.graph._
import graphlab.user._
import graphlab.engine._

class Master[VertexDataType,EdgeDataType,GatherType:Manifest] {
  
  type E = Edge[EdgeDataType,VertexDataType]
  type V = Vertex[VertexDataType]
  type S = Shard[VertexDataType,EdgeDataType,GatherType]

  var shards:Array[S] = null
  var verts:Array[V] = null
  val NUM_SHARDS = 256

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
	(num_edges,ss,dd,parse_input(data))
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

    num_verts = num_verts + 1 //yay off by one

    def make_vertex(id:Int):V = new graphlab.graph.Vertex(id,init_vertex(id))
    
    //all shards will have reference to this
    verts = (0 until num_verts).map(make_vertex).toArray

    def make_edge(x:(Int,Int,Int,EdgeDataType)):E = {
      verts(x._2).inc_out
      verts(x._3).inc_in
      new E(x._1,verts(x._2),verts(x._3),x._4)
    }

    //list of all edges
    val all_edges = edge_protoplasm.map(make_edge)

    def make_shard(id:Int):S = new graphlab.engine.Shard(id,verts)

    //create shards
    shards = (0 until NUM_SHARDS).map(make_shard).toArray

    def e_place(e:E):Unit = {
      shards(hash(e.id)).push_edge(e)
    }

    def v_place(v:V):Unit = {
      shards(hash(v.id)).push_vert(v)
    }

    //give shards appropriate edges and verts
    all_edges.map(e_place)
    verts.map(v_place)

  }

  def run_gas(
    gather:(V,E)=>(EdgeDataType,GatherType),
    sum:(GatherType,GatherType)=>GatherType,
    apply:(V,GatherType)=>VertexDataType,
    scatter:(V,E)=>(EdgeDataType,Boolean),
    default_gather:GatherType,
    gatherdir:Dir,scatterdir:Dir) = {

    println("starting GAS")

    var acc:Array[Either[GatherType,GatherType]] = null

    var signal:Array[Boolean] = (0 until verts.length).map(_ => true).toArray
    
    //count the iterations
    var iter = 0

    val lock:AnyRef = new AnyRef()

    shards.map(_.register_lock(lock))
    shards.map(_.register_signal(signal))

    //run shards until convergence
    while (signal.foldLeft(false)(_||_)) {

      println("starting gas iteration")

      /* GATHER PHASE */

      //reset accumulator to 0
      acc = ((0 until verts.length).map(_ => Left(default_gather))).toArray

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

      //kick off gather for all shards
      val gather_futures = shards.map(_.run_gather(gather,gatherdir,accumulate))

      //wait for all gathers to finish, and run accumulation
      gather_futures.map(_.apply())

      /* APPLY PHASE */

      //what to do when done applying
      def commit(x:(V,VertexDataType)):Unit = {
	x._1.data = x._2
      }

      //kick off apply for all shards
      val apply_futures = shards.map(_.run_apply(apply,acc,commit))
      
      //wait for all apply to finish
      apply_futures.map(_.apply())

      /* SCATTER PHASE */

      //reset signal to all false
      signal = (0 until verts.length).map(_ => false).toArray

      //what to do when done scattering
      def publish(x:((E,EdgeDataType),(V,Boolean))):Unit = {
	val (edge,data) = x._1
	val (vert,sig) = x._2
	signal(vert.id) |= sig
	edge.data = data
      }

      //kick off scatter for all shards
      val scatter_futures = shards.map(_.run_scatter(scatter,scatterdir,publish))

      //wait for scatter to finish
      scatter_futures.map(_.apply())

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

  
