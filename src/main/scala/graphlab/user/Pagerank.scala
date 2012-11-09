package graphlab.user

import graphlab.user._
import graphlab.engine._

object Pagerank {

  def main(args:Array[String]) = {
    
    val m:Master[(Double,Double),Double,Double] = new Master

    val RESET_PROB = .15
    val CONVERGENCE = .001

    m.build_graph(args(0),_.toDouble,()=>(1.0,5.0))

    m.run_gas((v,e) => {
      //gather
      val agg = e.data*e.source.data._1
      //println("Gathering " + e.source.id + "->" + e.target.id + " for v: " + v.id + " agg: " + agg)
      (e.data,agg)
    },
      (x,y) => x+y, //sum
      (v,g) => {
	//apply
	//println("applying: " + v.id + " : " + g)
        ((1-RESET_PROB)*g+(RESET_PROB),v.data._1)
      },
      (v,e) => {
	//scatter
	val err = math.abs(v.data._1-v.data._2)
	//println("scattering: " + e.source.id + "->" + e.target.id + " for v: " + v.id + ", error: " + err)
	(e.data,err > CONVERGENCE)
      },
      () => 0.0, //init gather type
      In,Out) //gather_edges, scatter_edges

    

  }
}
