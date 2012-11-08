package graphlab.engine

import scala.io.Source
import scala.collection.mutable.MutableList
import scala.actors.Futures._
import scala.math._
import graphlab.graph._
import graphlab.user._

class Master[VertexDataType,EdgeDataType,GatherType] {
  
  type GAS = VertexProgram[VertexDataType,EdgeDataType,GatherType]

  var user_program:GAS

  def register_prog(g:GAS) = { user_program = g }

  def build_graph(fname:String) = {

    val NUM_SHARDS = 256

    //Here is the user program
    val gas = user_program

    //read graph in from file
    val split_input = Source.fromFile(fname).mkString.split("\n").map(_.split("\t"))

    var num_verts = 0

    var num_edges = -1

    def make_edge(l:Array[java.lang.String]):Edge[EdgeDataType] = l.toList match {
      case List(s,d,data) => {
	num_verts = max(num_verts,s.toInt)
	num_verts = max(num_verts,d.toInt)
	num_edges = num_edges + 1
	new graphlab.graph.Edge(num_edges,s.toInt,d.toInt,data)
      }
      case _ => throw new RuntimeException("malformed input")
    }
	
    //list of all edges
    val all_edges = split_input.map(make_edge)

    def make_vertex(id:Int):Vertex[VertexDataType] = new graphlab.graph.Vertex(id,1.0)
    
    //all shards will have reference to this
    val verts = (0 until num_verts).map(make_vertex)

    def make_shard(id:Int):Shard[GatherType] = new graphlab.engine.Shard(id,gas,verts)

    //create shards
    val shards:Array[Shard[GatherType]] = (0 until NUM_SHARDS).map(make_shard)

    def e_place(e:Edge[EdgeDataType],n:Int):Unit = {
      shards(e.id % n).push_edge(e)
    }

    //give shards appropriate edges
    

  }

  def run_gas():Unit = {

    //run shards until convergence
    while (shards.foldLeft(false)((s,b)=>s.converged || b)) {
      //kick off gather for all shards
      val acc_futures = shards.map(_.run_gather)

    }    

  }

}

  
