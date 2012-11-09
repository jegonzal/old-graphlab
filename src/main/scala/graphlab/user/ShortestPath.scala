package graphlab.user

import graphlab.user._
import graphlab.engine._

class ShortestPath {

   def main(args:Array[String]) = {
    
    val m:Master[(Double,Double),Double,Double] = new Master

    m.build_graph(args(0),(s)=>Double(s),
        (id)=>((id == Int(args(1))) ? 0 : Int.MaxValue, (id == Int(args(1))) ? 0 : Int.MaxValue))

    m.run_gas((v,e) => (e.data+e.source.data,e.get_other_vertex(v).id), //gather
      math.min, //sum
      (v,g) => (math.min(g,v.data._1),v.data._1),	//apply
      (v,e) => (e.data,v.data._1 != v.data._2),	//scatter
      () => Int.MaxValue, //init gather type
      All,All) //gather_edges, scatter_edges

    m.dump_graph()
  }
   
}