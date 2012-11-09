package graphlab.user

import graphlab.user._
import graphlab.engine._

object ConnectedComponents {

  def main(args:Array[String]) = {
    
    val m:Master[(Int,Int),Int,Int] = new Master

    m.build_graph(args(0),(s)=>0,(id)=>(id,id))

    m.run_gas((v,e) => (e.data,e.get_other_vertex(v).id), //gather
      math.min, //sum
      (v,g) => (math.min(g,v.data._1),v.data._1),	//apply
      (v,e) => (e.data,v.data._1 != v.data._2),	//scatter
      () => Int.MaxValue, //init gather type
      All,All) //gather_edges, scatter_edges

    m.dump_graph()

  }
}
