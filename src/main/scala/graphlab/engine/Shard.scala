package graphlab.engine

import graphlab.user.GAS
import scala.actors.Future
import scala.actors.Futures._

class Shard[VertexDataType,EdgeDataType,GatherType](id:Int,gas:VertexProgram[VertexDataType,EdgeDataType,GatherType],verts:List[Vertex[VertexDataType]]) {

  var edges:List[Edge[EdgeDataType]]

  def run_gather:Future[List[GatherType]] = future {
    edges.map(gas.gather)
  }

  def push_edge(e:Edge[EdgeDataType]) = edges ::= e

  def converged = false

}  

