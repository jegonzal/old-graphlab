package graphlab.user

import graphlab.user._
import graphlab.engine._
import scala.collection.immutable.Set

object GraphColor {

  def main(args:Array[String]) = {
    
    val m:Master[(Int,Int),Int] = new Master

    m.build_graph(args(0),(s)=>0,(id)=>(id,id))
    
    m.run_gas[Set[Int]]((v,e) => (e.data,
        if(v.id > e.get_other_vertex(v).id) Set() else Set(e.get_other_vertex(v).data._1)
        ), //gather
      _|_, //sum
      (v,g) => {
    	  println("applying on vertex " + v.id + " with acc " + g)        	
          var c = 0
          while(g(c)) 
            c += 1
          println("Chose color " + c)
          (c,v.data._1)
      },	//apply
      (v,e) => (e.data,v.data._1 != v.data._2),	//scatter
      Set(), //init gather type
      All,All) //gather_edges, scatter_edges

    m.dump_graph()

  }
}
