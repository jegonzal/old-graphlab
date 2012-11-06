package graphlab.graph

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

class Graph[VT, ET] {
  
  class Edge(val source: Long, val target: Long, var value: ET) 
  
  class Vertex(var value: VT,
      val inEdges: ArrayBuffer[Edge] = new ArrayBuffer[Edge](), 
      val outEdges: ArrayBuffer[Edge] = new ArrayBuffer[Edge]()) {
  }
  val vertices = new HashMap[Long, Vertex] 

  val edges = new ArrayBuffer[Edge]();
  
  def addVertex(vid: Long, value: VT) = {
    if(vertices.contains(vid)) vertices(vid).value = value;
    else vertices(vid) = new Vertex(value);
  }

  def addEdge(source: Long, target: Long, value: ET) = {
    val edge = new Edge(source, target, value)
    vertices(source).outEdges += edge
    vertices(target).inEdges += edge
    edges += edge
  }
  
  def vertex(vid: Long) = vertices(vid);
  
}

object hello {
  def main(args: Array[String]) {
    val g = new Graph[String, String];
    g.addVertex(1, "hello")
    g.addVertex(2, "world")
    g.addVertex(3, "the")
    g.addVertex(4, "end")
    g.addEdge(1,2, " -> ");
    g.addEdge(2,3, " -> ");
    for((vid, vertex) <- g.vertices) println(vertex.value)
  }  
}


