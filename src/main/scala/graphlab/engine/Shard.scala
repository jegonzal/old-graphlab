package graphlab.engine

import graphlab.user._
import graphlab.graph._
import scala.actors.Future
import scala.actors.Futures._
import scala.collection.immutable._

class Shard[VertexDataType,EdgeDataType,GatherType](id:Int,gas:VertexProgram[VertexDataType,EdgeDataType,GatherType],verts:List[Vertex[VertexDataType]]) {

  type E = Edge[EdgeDataType]

  var edges:List[Edge[EdgeDataType]]

  def run_gather:Future[List[GatherType]] = {
    def g(e:E) = {
      gas.gather(e.target, e)
      gas.gather(e.source, e)
    }
    future { edges.map(g) }
  }

  def push_edge(e:Edge[EdgeDataType]) = edges ::= e

  def converged = false

}  

