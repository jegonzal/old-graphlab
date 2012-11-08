package org.graphlab.net;

import java.util.HashMap;
import org.graphlab.*;

/**
 * Created with IntelliJ IDEA.
 * User: akyrola
 * Date: 11/7/12
 * Time: 4:56 PM
 * To change this template use File | Settings | File Templates.
 */
public interface Master {

    void registerSlave(GraphLabNodeInfo node);

    void finishedStage(int nodeId, ExecutionPhase phase);

    HashMap<Integer, GraphLabNodeInfo> getNodes();

}
