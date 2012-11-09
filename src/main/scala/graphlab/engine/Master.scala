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

  def build_graph(fname:String,parse_input:(String)=>EdgeDataType,init_vertex:()=>VertexDataType) = {

    //read graph in from file
    val split_input = Source.fromFile(fname).mkString.split("\n").map(_.split("\t"))

    var num_verts = 0
    var num_edges = -1

    def make_edge_tuple(l:Array[java.lang.String]) = l.toList match {
      case List(s,d,data) => {
	num_verts = max(num_verts,s.toInt)
	num_verts = max(num_verts,d.toInt)
	num_edges = num_edges + 1
	(num_edges,s.toInt,d.toInt,parse_input(data))
      }
      case _ => throw new RuntimeException("malformed input")
    }
	
    //list of all tuples to construct edges
    val edge_protoplasm = split_input.map(make_edge_tuple)

    num_verts = num_verts + 1 //yay off by one

    def make_vertex(id:Int):V = new graphlab.graph.Vertex(id,init_vertex())
    
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
    init_gather:()=>GatherType,
    gatherdir:Dir,scatterdir:Dir) = {

    println("starting GAS")

    var acc:Array[GatherType] = ((0 until verts.length).map({_ => init_gather()})).toArray

    var signal:Array[Boolean] = (0 until verts.length).map(_ => true).toArray
    
    //count the iterations
    var iter = 0

    //run shards until convergence
    while (signal.foldLeft(false)(_||_)) {

      println("gas iteration")

      /* GATHER PHASE */

      //kick off gather for all shards
      val gather_futures = shards.map(_.run_gather(gather,gatherdir))

      acc = ((0 until verts.length).map({_ => init_gather()})).toArray

      //what to do when done gathering
      def accumulate(x:((V,GatherType),(E,EdgeDataType))):Unit = {
	val (vert,a) = x._1
	val (edge,data) = x._2
	acc(vert.id) = sum(acc(vert.id),a)
	edge.data = data
      }

      //wait for all gathers to finish, and runn accumulation
      gather_futures.map(_.apply().map(accumulate))

      /* APPLY PHASE */

      //kick off apply for all shards
      val apply_futures = shards.map(_.run_apply(apply,acc))

      //what to do when done applying
      def commit(x:(V,VertexDataType)):Unit = {
	x._1.data = x._2
      }
      
      //wait for all apply to finish
      apply_futures.map(_.apply().map(commit))

      /* SCATTER PHASE */

      //kick off scatter for all shards
      val scatter_futures = shards.map(_.run_scatter(scatter,scatterdir))

      //reset signal to all false
      signal = (0 until verts.length).map(_ => false).toArray

      //what to do when done scattering
      def publish(x:((E,EdgeDataType),(V,Boolean))):Unit = {
	val (edge,data) = x._1
	val (vert,sig) = x._2
	signal(vert.id) |= sig
	edge.data = data
      }
      
      //wait for scatter to finish
      scatter_futures.map(_.apply().map(publish))

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

  
