package graphlab
import spark.SparkContext
import spark.SparkContext._

object Pagerank {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[4]", "pagerank")
    val edges = sc.textFile("/Users/haijieg/tmp/google.tsv").map {
    	line => {val sp = line.split("\t"); sp(0).toInt ->sp(1).toInt} 
    }.cache()
    
    val maxiter = 1
    
    var vertices = edges.flatMap(e => List((e._1, 1.0), (e._2, 1.0))).distinct(16).cache()
    val outedges = edges.map(e => (e._1, 1)).reduceByKey(_ + _).cache()
    val inedges = edges.map(e => (e._2, 1)).reduceByKey(_ + _).cache()

    for (i <- 1 to maxiter) {
      // Begin iteration    
      System.out.println("Begin iteration %d", i);

      System.out.println("Join vdata with outdeg")
      var pr_deg = vertices.join(outedges)

      // gather for source    
      System.out.println("Gather on source")
      var saccu = edges.map(e => (e._2, e._1)).join(pr_deg).map { case (target, (src, (targetval, targetdeg))) => (target, targetval / targetdeg) }

      // gather for target
      System.out.println("Gather on target")
      var taccu = edges.join(pr_deg).map { case (src, (target, (srcval, srcdeg))) => (target, srcval / srcdeg) }

      // apply
      System.out.println("Apply")
      vertices = saccu.join(taccu).map { case (vid, (val1, val2)) => (vid, 0.15 + 0.85 * (val1 + val2)) }
    }
  }
}