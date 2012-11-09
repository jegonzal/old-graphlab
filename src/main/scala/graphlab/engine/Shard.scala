package graphlab.engine

import graphlab.user._
import graphlab.graph._
import scala.actors.Future
import scala.actors.Futures._
import scala.collection.immutable._

sealed trait Dir
case object In extends Dir
case object Out extends Dir
case object All extends Dir
case object None extends Dir

class Shard[VertexDataType,EdgeDataType,GatherType](id:Int,verts:Array[Vertex[VertexDataType]]) {

  type E = Edge[EdgeDataType,VertexDataType]
  type V = Vertex[VertexDataType]

  type ED = EdgeDataType
  type VD = VertexDataType
  type G = GatherType

  var edges:List[E] = List()

  var my_verts:List[V] = List()

  def run_gather(gather:(V,E)=>(ED,G),direction:Dir):Future[List[((V,G),(E,ED))]] = {
    def g(e:E) = {
      var l:List[((V,G),(E,ED))] = List()
      if (direction == In || direction == All) {
	val (a,b) = gather(e.target, e)
	l = ((e.target,b),(e,a))::l
      }
      if (direction == Out || direction == All) {
	val (c,d) = gather(e.source, e)
	l=((e.source,d),(e,c))::l
      }
      l
    }
    future { edges.flatMap(g) }
  }

  def run_apply(apply:(V,G)=>VD,acc:Array[G]):Future[List[(V,VD)]] = {
    def a(v:V) = {
      (v,apply(v,acc(v.id)))
    }
    future { my_verts.map(a) }
  }

  def run_scatter(scatter:(V,E)=>(ED,Boolean),direction:Dir):Future[List[((E,ED),(V,Boolean))]] = {
    def s(e:E) = {
      var l:List[((E,ED),(V,Boolean))] = List()
      if (direction == In || direction == All) {
	val (a,b) = scatter(e.target,e)
	l=((e,a),(e.source,b))::l
      }
      if (direction == Out || direction == All) {
	val (c,d) = scatter(e.source,e)
	l=((e,c),(e.target,d))::l
      }
      l
    }
    future { edges.flatMap(s) }
  }

  def push_edge(e:E) = edges ::= e

  def push_vert(v:V) = my_verts ::= v

}  

