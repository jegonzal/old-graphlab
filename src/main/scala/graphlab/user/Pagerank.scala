package graphlab.user

import graphlab.user._
import graphlab.engine._

object Pagerank {

  def main(args:Array[String]) = {
    
    val graph:Graph[(Double,Double),Double] = new Graph

    val RESET_PROB = .15
    val CONVERGENCE = .001

    graph.build_graph(args(0),(_)=>1.0,(_)=>(1.0,5.0))

    graph.run_gas[Double]((v,e) => {
      //gather
      val agg = (1 - RESET_PROB)*e.source.data._1/e.source.num_out_edges
      (e.data,agg)
    },
      (x,y) => x+y, //sum
      (v,g) => {
	//apply
        (g + RESET_PROB,v.data._1)
      },
      (v,e) => {
	//scatter
	val err = math.abs(v.data._1-v.data._2)
	(e.data,err > CONVERGENCE)
      },
      0.0, //default gather type
      In,Out) //gather_edges, scatter_edges

    graph.dump_graph()

    println(graph.map_reduce_verts[Double](
      (v)=>v.data._1,
      _+_))

  }
}
