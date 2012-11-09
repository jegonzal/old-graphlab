package graphlab.user

import graphlab.user._
import graphlab.engine._

object Pagerank {

  def main(args:Array[String]) = {
    
    val m:Master[(Double,Double),Double] = new Master

    val RESET_PROB = .15
    val CONVERGENCE = .001

    m.build_graph(args(0),(_)=>1.0,(_)=>(1.0,5.0))

    m.run_gas[Double]((v,e) => {
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

    m.dump_graph()

    println(m.map_reduce_verts[Double](
      (v)=>v.data._1,
      _+_))

  }
}
