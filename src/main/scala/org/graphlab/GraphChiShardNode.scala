package org.graphlab

import net.netty.messages.FinishedPhaseMessage
import net.{Master, GraphLabNode}
import net.netty.{DataExchange, SlaveImplementation}
import java.net.InetAddress
import edu.cmu.graphchi.engine.GraphChiEngine
import edu.cmu.graphchi.shards.MemoryShard
import edu.cmu.graphchi.{ChiVertex, ChiFilenames}
import edu.cmu.graphchi.datablocks.FloatConverter
import collection.mutable.ArrayBuffer
import edu.cmu.graphchi.engine.auxdata.{VertexDegree, DegreeData}
import java.util.concurrent.atomic.AtomicInteger

/**
 * Node that has one graphchi shard.
 */
class GraphChiShardNode(id: Int, masterHost: String, graphName: String, nodes: Int) extends GraphLabNode {
  type VertexType = java.lang.Float
  type EdgeType = java.lang.Float

  type GatherType = java.lang.Float

  val edgeConverter = new FloatConverter()
  val vertexValueConverter = new FloatConverter()
  val gatherValueConverter = new FloatConverter()


  /* Load the shard */
  val graphchi = new GraphChiEngine[VertexType, EdgeType](graphName, nodes)
  val interval = graphchi.getIntervals.get(id)
  val myShard = new MemoryShard[EdgeType](ChiFilenames.getFilenameShardEdata(graphName,
    edgeConverter, id,  nodes), ChiFilenames.getFilenameShardsAdj(graphName, id, nodes),
    interval.getFirstVertex, interval.getLastVertex)
  myShard.setConverter(edgeConverter)
  myShard.setOnlyAdjacency(true)

  val vertexArray = ArrayBuffer.fill(graphchi.numVertices())(0.0f)

  /* Start comm */
  val netSlave = new SlaveImplementation(this, id,  masterHost,
    InetAddress.getLocalHost.getHostAddress, 4011 + id)

  netSlave.start()

  val dataExchange = new DataExchange[VertexType, GatherType](vertexValueConverter, gatherValueConverter)
  dataExchange.registerDecoders()


  var curVertices : Array[ChiVertex[_,_]] = null
  var curGathers : ArrayBuffer[GatherType] = null
  val degreeData = new DegreeData(graphName)

  val shuffleCounter = new AtomicInteger(0)
  val otherNodes = (0 until nodes).filter(i => i != id)

  var curPhase : ExecutionPhase = null
  var curFrom = 0
  var curTo = 0

  def remoteStartPhase(phase: ExecutionPhase, fromVertex: Int, toVertex: Int) {
    curPhase = phase
    curFrom = fromVertex
    curTo = toVertex
    phase.getPhaseNum match {
      case ExecutionPhase.GATHER =>
        shuffleCounter.set(1)
        degreeData.load(fromVertex, toVertex)
        curGathers = ArrayBuffer.fill(toVertex - fromVertex + 1)(0.0f)
        curVertices = new Array[ChiVertex[_,_]](toVertex - fromVertex + 1)
        var i : Int = 0
        while( i < curVertices.length) {     // Unscalaish, but faster
          curVertices(i) = new GASVertex(i + fromVertex)
          i += 1
        }

        // Gather happens during load
        myShard.loadVertices(fromVertex, toVertex, curVertices, false)

        // shuffle gathers
        System.out.println("Going to shuffle")
        val vertexIds = (fromVertex until toVertex+1).toArray
        val gathersArray = curGathers.toArray
        otherNodes.foreach(otherNode => netSlave.sendToNode(otherNode,   // TODO, send only vertex master
          dataExchange.getMessageForGatherArray(vertexIds, gathersArray)
        ))

      case ExecutionPhase.APPLY =>
        var i : Int = 0
        while( i < curVertices.length) {     // Unscalaish, but faster
          vertexArray(fromVertex + i) =
             (curGathers(i) + 0.15f) / degreeData.getDegree(fromVertex + i).outDegree //  Vertex degree
          i += 1
        }

      case ExecutionPhase.SCATTER =>
    }
  }

  class GASVertex(vid : Int) extends ChiVertex[VertexType, EdgeType](id, new VertexDegree(0,0)) {
    override def addInEdge(chunkId: Int, offset: Int, vertexId: Int) {
      // Pagerank
      curGathers(vid) += 0.85f * vertexArray(vertexId)
    }

    override def addOutEdge(chunkId: Int, offset: Int, vertexId: Int) {
      // Not done
    }
  }

  def remoteReceiveVertexData(fromNode: Int, vertexIds: Array[Int], vertexData: Array[AnyRef]) {

  }

  def remoteReceiveGathers(fromNode: Int, vertexIds: Array[Int], gatherData: Array[AnyRef]) {
    println("Received gathers: " + vertexIds.length + ", " + gatherData.length)
    if (shuffleCounter.incrementAndGet() == nodes) {
        // Next phase
        netSlave.sendToMaster(new FinishedPhaseMessage(curPhase, curFrom, curTo))
    }

  }
}

object GraphChiShardNode {
  def main(args: Array[String]) {
    val graphName = args(0)
    val shards = args(1).toInt

    (0 until shards).foreach(shardNum => {
      val node = new GraphChiShardNode(shardNum, args(2), graphName, shards)
    })
  }

}