package graphlab.engine

import scala.io.Source
import scala.collection.mutable.MutableList
import scala.actors.Futures._
import scala.math._
import graphlab.graph._
import graphlab.user._

class Master[VertexDataType,EdgeDataType,GatherType](user_program:VertexProgram[VertexDataType,EdgeDataType,GatherType]) {
  
  type GAS = VertexProgram[VertexDataType,EdgeDataType,GatherType]

  type E = Edge[EdgeDataType]
  type V = Vertex[VertexDataType]
  type S = Shard[VertexDataType,EdgeDataType,GatherType]

  var shards:List[S]

  def build_graph(fname:String) = {

    val NUM_SHARDS = 256

    //Here is the user program
    val gas = user_program

    //read graph in from file
    val split_input = Source.fromFile(fname).mkString.split("\n").map(_.split("\t"))

    var num_verts = 0
    var num_edges = -1
    def make_edge(l:Array[java.lang.String]):E = l.toList match {
      case List(s,d,data) => {
	num_verts = max(num_verts,s.toInt)
	num_verts = max(num_verts,d.toInt)
	num_edges = num_edges + 1
	new graphlab.graph.Edge(num_edges,s.toInt,d.toInt,gas.parse_input(data))
      }
      case _ => throw new RuntimeException("malformed input")
    }
	
    //list of all edges
    val all_edges = split_input.map(make_edge)

    def make_vertex(id:Int):V = new graphlab.graph.Vertex(id,gas.init_vertex)
    
    //all shards will have reference to this
    val verts:List[V] = (0 until num_verts).map(make_vertex).toList

    def make_shard(id:Int):S = new graphlab.engine.Shard(id,gas,verts)

    //create shards
    shards = (0 until NUM_SHARDS).map(make_shard).toList

    def e_place(e:E):Unit = {
      shards(e.id % NUM_SHARDS).push_edge(e)
    }

    //give shards appropriate edges
    all_edges.map(e_place)

  }

  def run_gas():Unit = {

    //run shards until convergence
    while (shards.foldLeft(false)((b,s)=>s.converged || b)) {
      //kick off gather for all shards
      val acc_futures = shards.map(_.run_gather)

    }    

  }

}

  
