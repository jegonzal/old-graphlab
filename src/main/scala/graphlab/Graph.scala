package graphlab
import spark.SparkContext
import spark.SparkContext._


class Graph[VD, ED] (
    val vertices: spark.RDD[(Int,VD)],
    val edges: spark.RDD[(Int, Int, ED)]) {

  def cache { edges.cache}

  def iterateGAS[A](
      gather: ((Int, VD), ED, (Int, VD)) => (A, ED),
      sum: (A,A) => A,
      apply: ((Int, VD), A) => VD,
      scatter: ((Int, VD), ED, (Int, VD)) => ED,
      niter: Int,
      gather_edges: String  = "in",
      scatter_edges: String = "out") = {
    
    
    
    
  }
}

object Graph {
  def load_graph[ED](sc: SparkContext,
      fname: String, edge_parser: String => ED) = {
    val edges = sc.textFile(fname).map{
      line => {
        val source::target::tail = line.split("\t").toList; 
          (source.trim.toInt, target.trim.toInt, 
              edge_parser(tail.mkString("\t")))
        } 
      } 
    
    val vertices = edges.flatMap{ 
      case (source, target, _) => List((source, 1), (target,1))
    }.reduceByKey(_ + _)

    new Graph[Int, ED](vertices, edges);
  }
  
  
}

object GraphTest {
  def main(args : Array[String]) {
    
  }
}