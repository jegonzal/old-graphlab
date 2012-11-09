package org.graphlab

import net.{GraphLabNodeInfo, Master}
import net.netty.MasterServer
import org.graphlab.net.netty.messages._
import org.graphlab.net.netty._
import java.util.concurrent.atomic.AtomicInteger
import edu.cmu.graphchi.engine.GraphChiEngine
import edu.cmu.graphchi.datablocks.FloatConverter

/**
 * Super simple master
 */
object MasterImpl extends Master {

  type VertexType = java.lang.Float
  type GatherType = java.lang.Float

  val vertexValueConverter = new FloatConverter()
  val gatherValueConverter = new FloatConverter()

  val server = new MasterServer(MasterImpl)
  var nodes : Set[Int] = null
  val countDown = new AtomicInteger()
  val resultCountDown = new AtomicInteger()
  var numVertices = 0
  var running = false
  var results:List[Tuple2[Int, VertexType]] = List()

  def transformToPhase(phase: ExecutionPhase, fromVertex: Int, toVertex: Int) {
    countDown.set(nodes.size)

    nodes.foreach(nodeId => server.sendClient(nodeId, new ExecutePhaseMessage(phase, fromVertex, toVertex)))

    // Dirty
    while(countDown.get() > 0 && running) {
      Thread.sleep(50)
    }
  }

  def showTop(topN: Int = 12) {
    resultCountDown.set(nodes.size)
    results = List[Tuple2[Int, VertexType]]()
    nodes.foreach(nodeId => server.sendClient(nodeId, new TopResultsQuery(topN)))
  }


  def remoteReceiveTopResults(vertexIds: Array[Int], vertexData: Array[AnyRef]) {
      this.synchronized {
         results = results ++ vertexIds.zip(vertexData.map(_.asInstanceOf[java.lang.Float]))
      }
    val cnt = resultCountDown.decrementAndGet()

    if (cnt == 0) {
       printf("Top:\n")
       val topResults = results.sortWith(_._2 > _._2).take(12)
       topResults.foreach(x => println(x._1 + ", " + x._2))
    } else printf("Received results, still waiting from %d\n", cnt)
  }

  def runIteration(iteration: Int) {
    if (running) {
      showTop()

      val stepSize = 50000
      (0 until numVertices by stepSize).foreach(from => {
        val fromVertex = from
        val toVertex = scala.math.min(from + stepSize, numVertices)
        transformToPhase(new ExecutionPhase(ExecutionPhase.GATHER), fromVertex, toVertex)
        transformToPhase(new ExecutionPhase(ExecutionPhase.APPLY), fromVertex, toVertex)
        transformToPhase(new ExecutionPhase(ExecutionPhase.SCATTER), fromVertex, toVertex)
      })
    } else {
      println("Skipped iteration, as not running")
    }
  }


  def remoteRegisterSlave(node: GraphLabNodeInfo) {}

  def remoteUnregisterSlave(node: GraphLabNodeInfo) {
    println("Finishing, one node unregistered")

  }

  def remoteFinishedPhase(nodeId: Int, phase: ExecutionPhase) {
    countDown.decrementAndGet()
  }

  def start(numNodes: Int, numV: Int) {
    server.start()
    numVertices = numV

    new DataExchange[VertexType, GatherType](vertexValueConverter, gatherValueConverter).registerDecoders()

      printf("Waiting for %d nodes", numNodes)
      running = false
      while(server.getNumOfRegisteredNodes < numNodes) Thread.sleep(100)

      running = true
      nodes = (0 until numNodes).toSet
      printf("OK - ready\n")
  }

  def run(numIterations: Int) {
    (0 until numIterations).foreach(iteration => runIteration(iteration))
  }

  def main(args: Array[String]) {
    val graphName = args(0)
    val shards = args(1).toInt

    val engine =
      new GraphChiEngine[java.lang.Float, java.lang.Float](graphName, shards)
    val numVertices = engine.numVertices()
    start(shards, numVertices)
  }


}
