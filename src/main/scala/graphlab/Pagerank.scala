package graphlab
import spark.SparkContext
import spark.SparkContext._
import spark.HashPartitioner
import spark.storage.StorageLevel

object Pagerank {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[4]", "pagerank")
    val numprocs = 4

    // (source, target)
    val edges = sc.textFile("/Users/haijieg/tmp/google.tsv").sample(false, 1, 1).map {
    	line => {val sp = line.split("\t"); sp(0).trim.toInt ->sp(1).trim.toInt} 
    }.partitionBy(new HashPartitioner(numprocs)).persist(StorageLevel.DISK_ONLY)

    // (target, source)
    val edgesinv = edges.map(e => (e._2, e._1)).partitionBy(new HashPartitioner(numprocs)).persist(StorageLevel.DISK_ONLY)
    
    // (vid, vdata)
    var vertices = edges.flatMap(e => List((e._1, 1.0), (e._2, 1.0))).distinct(numprocs)        
    
    val outedges = edges.mapValues(_ => 1).reduceByKey(_ + _)
    val inedges = edgesinv.mapValues(_ => 1).reduceByKey(_ + _)
    
    // (vid, (inedges, outedges))
    val vcontext = vertices.leftOuterJoin(inedges).leftOuterJoin(outedges).mapValues {
      case ((value, inedges), outedges) => (inedges.getOrElse(0), outedges.getOrElse(0)) 
    }.cache()
    
    val maxiter = 20
    for (i <- 1 to maxiter) {
      // Begin iteration    
      System.out.println("Begin iteration:" + i);

      var vinfo = vertices.join(vcontext)
      
     // gather out edges
      /*
      System.out.println("Gather on source")
      var saccu = edgesinv.join(vinfo).map { 
        case (target, (src, (targetval, (indeg, outdeg)))) => ???)
      }	
      */				

      // gather in edges    
      System.out.println("Gather in edges")
      var taccu = edges.join(vinfo).map {
        case (src, (target, (srcval, (indeg, outdeg)))) => (target, (srcval / outdeg))
      }.reduceByKey(_ + _)

      // apply
      System.out.println("Apply")
      vertices = taccu.mapValues { val1 => (0.15 + 0.85 * val1) }
      
      vertices.take(10).foreach(println)
    }
  }
}