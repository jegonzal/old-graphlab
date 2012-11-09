package graphlab.user

import graphlab.user._
import graphlab.engine._
import scala.collection.immutable.Set

object TriangleCount {

  def main(args:Array[String]) = {
    
    val m:Master[(Set[Int],Int),Int] = new Master

    m.build_graph(args(0),(s)=>0,(id)=>(Set(),0))

    // Count the size of the intersection
    m.run_gas[Set[Int]]((v,e) => (e.data,Set(e.get_other_vertex(v).id)), //gather
      _|_, //sum
      (v,g) => (g,0),	//apply
      (v,e) => ((v.data._1 & e.get_other_vertex(v).data._1).size,false),	//scatter
      Set(), //init gather type
      All,All) //gather_edges, scatter_edges
      
    // store the number of triangles for each node
    m.run_gas[Int]((v,e) => (e.data,e.data), //gather
      _+_, //sum
      (v,g) => (v.data._1,g/2),	//apply
      (v,e) => (e.data,false),	//scatter
      0, //init gather type
      All,All) //gather_edges, scatter_edges
      
    m.dump_graph()

  }
}
