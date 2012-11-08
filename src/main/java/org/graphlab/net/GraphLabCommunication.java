package org.graphlab.net;

/**
 * Interface for Graphlab communication (high level)
 */
public interface GraphLabCommunication {


    void registerSlave(GraphLabNodeInfo node);

    GraphLabNodeInfo getMaster();


    void enterBarrier(String barrierName);

    <VT> void sendVertexData(int nodeId, int[] vertexIds, VT[] vertexData);

    <GT> void sendGathers(int nodeId, int[] vertexIds, GT[] vertexData);

}
