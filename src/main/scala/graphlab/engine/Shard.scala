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
case object NoEdges extends Dir

class Shard[VertexDataType,EdgeDataType](id:Int) {

  type E = Edge[EdgeDataType,VertexDataType]
  type V = Vertex[VertexDataType]

  type ED = EdgeDataType
  type VD = VertexDataType


  def map_reduce_edges[G](mapFun:(E)=>G,accum:(G,G)=>G):Future[List[G]] = {
    future {
      edges.map(mapFun) match {
	case List() => List()
	case g => List(g.tail.foldLeft(g.head)(accum))
      }
    }
  }

  def map_reduce_verts[G](mapFun:(V)=>G,accum:(G,G)=>G):Future[List[G]] = {
    future {
      verts.map(mapFun) match {
	case List() => List() 
	case g => List(g.tail.foldLeft(g.head)(accum))
      }
    }
  }
  
  def transform_verts(mapFun:(V)=>VertexDataType):Future[Unit] = {
    future {
      def f(v:V):Unit = {
	v.data = mapFun(v)
      }
      verts.foreach(f)
    }
  }

  def transform_edges(mapFun:(E)=>EdgeDataType):Future[Unit] = {
    future {
      def f(e:E):Unit = {
	e.data = mapFun(e)
      }
      edges.foreach(f)
    }
  }

  def run_gather[G](gather:(V,E)=>(ED,G),direction:Dir,accumulate:(((V,G),(E,ED)))=>Unit):Future[Unit] = {
    def g(e:E) = {
      var l:List[((V,G),(E,ED))] = List()
      if ((direction == In || direction == All) && signal(e.target.id)) {
	val (a,b) = gather(e.target, e)
	l = ((e.target,b),(e,a))::l
      }
      if ((direction == Out || direction == All) && signal(e.source.id)) {
	val (c,d) = gather(e.source, e)
	l=((e.source,d),(e,c))::l
      }
      l
    }
    future { 
      val fut = edges.flatMap(g)
      lock.synchronized {
	fut.map(accumulate)
      }
    }
  }

  def run_apply[G](apply:(V,G)=>VD,acc:Array[Either[G,G]],commit:((V,VD))=>Unit):Future[Unit] = {
    def a(v:V):List[(V,VD)] = {
      if (signal(v.id)) {
	acc(v.id) match {
	  case Left(n) => List((v,apply(v,n)))
	  case Right(n) => List((v,apply(v,n)))
	}
      } else {
	List()
      }
    }
    future { 
      val v = verts.flatMap(a) 
      lock.synchronized {
	v.map(commit)
      }
    }
  }

  def run_scatter[G](scatter:(V,E)=>(ED,Boolean),direction:Dir,publish:(((E,ED),(V,Boolean)))=>Unit):Future[Unit] = {
    def s(e:E) = {
      var l:List[((E,ED),(V,Boolean))] = List()
      if ((direction == In || direction == All) && signal(e.target.id)) {
	val (a,b) = scatter(e.target,e)
	l=((e,a),(e.source,b))::l
      }
      if ((direction == Out || direction == All) && signal(e.source.id)) {
	val (c,d) = scatter(e.source,e)
	l=((e,c),(e.target,d))::l
      }
      l
    }
    future { 
      val e = edges.flatMap(s) 
      lock.synchronized {
	e.map(publish)
      }
    }
  }

  //used for constructing shards
  def push_edge(e:E) = edges ::= e
  def push_vert(v:V) = verts ::= v

  //all shards share one global lock
  //synchronize on that lock when running callbacks at the end of phases
  var lock:AnyRef = null
  var signal:Array[Boolean] = null

  def register_lock(l:AnyRef) = { lock = l }
  def register_signal(s:Array[Boolean]) = { signal = s }

  //edges and vertices that this shard owns
  var edges:List[E] = List()
  var verts:List[V] = List()

}  

