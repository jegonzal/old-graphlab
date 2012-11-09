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

  val vertexArray = Array.fill(graphchi.numVertices())(0.0f.asInstanceOf[VertexType])

  /* Start comm */
  val netSlave = new SlaveImplementation(this, id,  masterHost,
    InetAddress.getLocalHost.getHostAddress, 4011 + id)

  netSlave.start()

  val dataExchange = new DataExchange[VertexType, GatherType](vertexValueConverter, gatherValueConverter)
  dataExchange.registerDecoders()


  var curVertices : Array[ChiVertex[_,_]] = null
  var curGathers : Array[GatherType] = null
  val degreeData = new DegreeData(graphName)

  val shuffleCounter = new AtomicInteger(1)
  val otherNodes = (0 until nodes).filter(i => i != id)

  var curPhase : ExecutionPhase = null
  var curFrom = 0
  var curTo = 0

  def vertexIdsForNodeInInterval(nodeId: Int, fromVertex: Int, toVertex:Int) = {
      val start = scala.math.max(fromVertex, graphchi.getIntervals.get(nodeId).getFirstVertex)
      val end = scala.math.min(toVertex, graphchi.getIntervals.get(nodeId).getLastVertex)
      (start to end)
  }

  def remoteStartPhase(phase: ExecutionPhase, fromVertex: Int, toVertex: Int) {
    curPhase = phase
    curFrom = fromVertex
    curTo = toVertex
    phase.getPhaseNum match {
      case ExecutionPhase.GATHER =>
        degreeData.load(fromVertex, toVertex)
        curGathers = Array.fill(toVertex - fromVertex + 1)(0.0f)
        curVertices = new Array[ChiVertex[_,_]](toVertex - fromVertex + 1)
        var i : Int = 0
        while( i < curVertices.length) {     // Unscalaish, but faster
          curVertices(i) = new GASVertex(i + fromVertex)
          i += 1
        }

        // Gather happens during load
        myShard.loadVertices(fromVertex, toVertex, curVertices, false)

        // shuffle gathers
        val gathersArray = curGathers.toArray
        otherNodes.foreach(otherNode => {
          val verticesOwnedByNode = vertexIdsForNodeInInterval(otherNode, curFrom, curTo)
          netSlave.sendToNode(otherNode,   // TODO, send only vertex master
            dataExchange.getMessageForGatherArray(verticesOwnedByNode.toArray,
              verticesOwnedByNode.map(vid => gathersArray(vid - curFrom)).toArray)
          )
          }
        )

      case ExecutionPhase.APPLY =>
        val verticesOwnedByNode = vertexIdsForNodeInInterval(this.id, curFrom, curTo)
        verticesOwnedByNode.foreach( vid => {     // Unscalaish, but faster
          if (vid < vertexArray.length) {
            vertexArray(vid) =
              (curGathers(vid - curFrom) + 0.15f) / degreeData.getDegree(vid).outDegree //  Vertex degree
          }
        })
        // Send my vertex values
        val myVertexValues = verticesOwnedByNode.map(i => vertexArray(i)).toArray[VertexType]
        otherNodes.foreach(otherNode => netSlave.sendToNode(otherNode,   // TODO, send only vertex master
          dataExchange.getMessageForVertexArray(verticesOwnedByNode.toArray, myVertexValues)
        ))


      case ExecutionPhase.SCATTER =>
        printf("Vertex %d = %f\n", 8737, vertexArray(8737))

        netSlave.sendToMaster(new FinishedPhaseMessage(curPhase, curFrom, curTo))
    }
  }

  class GASVertex(vid : Int) extends ChiVertex[VertexType, EdgeType](id, new VertexDegree(0,0)) {
    override def addInEdge(chunkId: Int, offset: Int, vertexId: Int) {
      // Pagerank
      curGathers(vid - curFrom) += 0.85f * vertexArray(vertexId)
    }

    override def addOutEdge(chunkId: Int, offset: Int, vertexId: Int) {
      // Not done
    }
  }

  def remoteReceiveVertexData(fromNode: Int, vertexIds: Array[Int], vertexData: Array[AnyRef]) {
    // Add gathers
    var j = 0
    // Unscalaish but faster
    while(j < vertexIds.length) {
      vertexArray(vertexIds(j)) = vertexData(j).asInstanceOf[VertexType]
      j+=1
    }

    val cnt = shuffleCounter.incrementAndGet()
    printf("Node %d, counter %d/%d, Received vertex data!\n", this.id, cnt, nodes)

    if (cnt == nodes) {
      // Next phase
      printf("Node %d finishing phase\n", id)
      shuffleCounter.set(1)

      netSlave.sendToMaster(new FinishedPhaseMessage(curPhase, curFrom, curTo))
    }
  }

  def remoteReceiveGathers(fromNode: Int, vertexIds: Array[Int], gatherData: Array[AnyRef]) {
    // Add gathers
    var j = 0
    // Unscalaish but faster
    while(j < vertexIds.length) {
      curGathers(vertexIds(j) - curFrom) += gatherData(j).asInstanceOf[GatherType]
      j+=1
    }

    val cnt = shuffleCounter.incrementAndGet()

    if (cnt == nodes) {
      // Next phase
      printf("Node %d finishing phase\n", id)
      shuffleCounter.set(1)

      netSlave.sendToMaster(new FinishedPhaseMessage(curPhase, curFrom, curTo))
    }

  }

  def remoteTopResultsRequested(topN: Int) {
    val verticesOwnedByNode = vertexIdsForNodeInInterval(this.id, 0, Int.MaxValue)
    val queryDegree = new DegreeData(graphName)
    queryDegree.load(verticesOwnedByNode.head, verticesOwnedByNode.last)

    val normalized = verticesOwnedByNode.map(i => (i, vertexArray(i) * queryDegree.getDegree(i).outDegree))
    val top = normalized.sortWith(_._2 > _._2).take(topN)

    val topIds = top.map(_._1).toArray
    val topValues = top.map(_._2.asInstanceOf[VertexType]).toArray.asInstanceOf[Array[VertexType]]
    netSlave.sendToMaster(dataExchange.getMessageForTopQueryResult(topIds, topValues))
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