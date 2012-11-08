package org.graphlab.net;

import org.graphlab.ExecutionPhase;
import sun.rmi.rmic.iiop.ValueType;

/**
 */
public interface GraphLabNode {

    <VT> void remoteReceiveVertexData(int fromNode, int[] vertexIds, VT[] vertexData);

    <GT> void remoteReceiveGathers(int fromNode, int[] vertexIds, GT[] vertexData);

    void remoteStartPhase(ExecutionPhase phase, int fromVertex, int toVertex);

}
