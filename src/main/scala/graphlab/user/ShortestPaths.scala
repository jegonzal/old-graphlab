package graphlab.user

import graphlab.user._
import graphlab.engine._

object ShortestPaths {

  def main(args:Array[String]) = {
    
    val graph:Graph[(Double,Double),Double] = new Graph

    graph.build_graph(args(0),(s)=>s.toDouble,(id)=>((if (id == args(1).toInt) 0.0 else -1.0), -1.0))
    
    def min_positive(x: Double, y: Double) = { 
	  (x>y, x>=0.0, y>=0.0) match {
	  case (true, _, true) => y
	  case (true, _, false) => x
	  case (false, true, true) => x
	  case (_, false, true) => y
	  case (false, _, false) => y
	  }
  }
    
    graph.run_gas[Double]((v,e) => (e.data,e.get_other_vertex(v).data._1+e.data), //gather
      min_positive, //sum
      (v,g) => (min_positive(g,v.data._1),v.data._1),	//apply
      (v,e) => (e.data,v.data._1 != v.data._2),	//scatter
      -1.0, //init gather type
      All,All) //gather_edges, scatter_edges

    graph.dump_graph()

  }
}
