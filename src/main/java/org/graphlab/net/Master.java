package org.graphlab.net;

import java.util.HashMap;
import org.graphlab.*;

/**
 */
public interface Master {

    void remoteRegisterSlave(GraphLabNodeInfo node);

    void remoteUnregisterSlave(GraphLabNodeInfo node);

    void remoteFinishedPhase(int nodeId, ExecutionPhase phase);

    void remoteReceiveTopResults(int[] vertexIds, Object[] vertexData);
}
