package org.graphlab.net;

import org.graphlab.ExecutionPhase;
import sun.rmi.rmic.iiop.ValueType;

/**
 */
public interface GraphLabNode {

    <VT> void receiveVertexData(int fromNode, int[] vertexIds, VT[] vertexData);

    <GT> void receiveGathers(int fromNode, int[] vertexIds, GT[] vertexData);


    void startPhase(ExecutionPhase phase, int fromVertex, int toVertex);

}
