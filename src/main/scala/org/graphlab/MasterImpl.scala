package org.graphlab

import net.{GraphLabNodeInfo, Master}
import net.netty.MasterServer
import net.netty.messages.ExecutePhaseMessage
import org.graphlab.net.netty._
import java.util.concurrent.atomic.AtomicInteger
import edu.cmu.graphchi.engine.GraphChiEngine

/**
 * Super simple master
 */
object MasterImpl extends Master {

  val server = new MasterServer(MasterImpl)
  var nodes : Set[Int] = null
  val countDown = new AtomicInteger()
  var numVertices = 0

  def transformToPhase(phase: ExecutionPhase, fromVertex: Int, toVertex: Int) {
    nodes.foreach(nodeId => server.sendClient(nodeId, new ExecutePhaseMessage(phase, fromVertex, toVertex)))
    countDown.set(nodes.size)

    // Dirty
    while(countDown.get() > 0) {
      print(".")
      Thread.sleep(50)
    }
  }

  def runIteration(iteration: Int) {
    val stepSize = 50000


    (0 until numVertices by stepSize).foreach(from => {
       val fromVertex = from
       val toVertex = scala.math.min(from + stepSize, numVertices)
       transformToPhase(new ExecutionPhase(ExecutionPhase.GATHER), fromVertex, toVertex)
       transformToPhase(new ExecutionPhase(ExecutionPhase.APPLY), fromVertex, toVertex)
       transformToPhase(new ExecutionPhase(ExecutionPhase.SCATTER), fromVertex, toVertex)
    })
  }


  def remoteRegisterSlave(node: GraphLabNodeInfo) {}

  def remoteFinishedPhase(nodeId: Int, phase: ExecutionPhase) {
    printf("Node id %d finished phase: %d", nodeId, phase.getPhaseNum)
    countDown.decrementAndGet()
  }

  def start(numNodes: Int, numV: Int) {
    server.start()
    numVertices = numV

    printf("Waiting for %d nodes", numNodes)
    while(server.getNumOfRegisteredNodes < numNodes) Thread.sleep(100)

    nodes = (0 until numNodes).toSet
    printf("OK - starting\n")
    println("Nodes: " + nodes)

    (0 until 5).foreach(iteration => runIteration(iteration))
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
