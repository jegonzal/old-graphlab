package graphlab.spark

import spark.SparkContext
import spark.SparkContext._
import spark.HashPartitioner
import spark.storage.StorageLevel

class MyPartitioner (numprocs:Int) extends spark.Partitioner {
  val numPartitions = numprocs;
  def getPartition(key: Any): Int = {
    key match {
      case (pid:Int, vid:Int) => pid
      case _ => 0
    }
  }
  override
  def equals(other: Any): Boolean = other.isInstanceOf[MyPartitioner]
}

object Pagerank {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[4]", "pagerank")
    val numprocs = 4
    val partitioner = new MyPartitioner(numprocs)

    // (source, target, pid)
    val edges = sc.textFile("/Users/haijieg/tmp/google.tsv").sample(false, 1, 1).map {
    	line => {
    	  val sp = line.split("\t");
    	  val src = sp(0).trim.toInt
    	  val dst = sp(1).trim.toInt
    	  val pid = Math.abs((src, dst).hashCode() % numprocs)
    	  ((pid, src), dst)
    	} 
    }.partitionBy(partitioner).persist(StorageLevel.MEMORY_ONLY)
 
    // ((pid, vid), vdata)
    var vertices = edges.flatMap {
      case ((pid, src), dst) => List(((pid, src), 1.0.toFloat), ((pid, dst), 1.0.toFloat))
    }.distinct(numprocs).partitionBy(partitioner)
    
    val vlocale = vertices.map{ case ((pid, vid), vdata) => (vid, pid) }.cache()
        
    val maxiter = 5
    
    def gather (src:Int, srcval:Float) (target:Int, targetval:Float) = ((targetval + srcval / 2).toFloat)
    def sum (a:Float, b:Float) :Float = { a+b }
    def apply (acc:Float) : Float = { (0.15 + 0.85 * acc).toFloat }
    
    for (i <- 1 to maxiter) {
      // Begin iteration    
      System.out.println("Begin iteration:" + i);
	      	
      // gather in edges    
      System.out.println("Gather in edges")      
      val ingather = edges.join(vertices)
      					.map { case ((pid, src), (target, srcval)) => ((pid, target), (src,srcval))}
      					.groupByKey(partitioner) // ((pid, target), seq[src, srcval])
      					.join(vertices) // ((pid, target), (seq[src, srcval], targetval))
      					.map { case ((pid,target), (srclist, targetval)) => {
      					  val accu = srclist.map {
      					    case (src, srcval) => gather (target, targetval) (src, srcval)
      					  }.reduce(sum)
      					  ((pid, target), accu)
      					  }   					
      					}
      
      System.out.println("Gather sum and Apply")          
      val vsync = ingather.map{case ((pid, vid), acc) => (vid, acc)}
      					  .reduceByKey(_ + _) // (vid, vdata)
      					  // apply      
      					  .mapValues(apply) // (vid, vdata)
      					  
      // Synchronize globally		
      System.out.println("Apply sync")          
      vertices =  vlocale.join(vsync)	// (vid, (pid, acc))
      					 .map {case (vid, (pid, acc)) => ((pid, vid), acc)} // ((pid, vid), acc)
      					 .partitionBy(partitioner).cache()
      vertices.take(10).foreach(println)
    }
  }
}
