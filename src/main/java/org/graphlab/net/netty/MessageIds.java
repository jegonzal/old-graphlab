package org.graphlab.net.netty;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class MessageIds {

    private static Map<Short, GraphLabMessageDecoder> decoders = new HashMap<Short, GraphLabMessageDecoder>();

    public static final short HANDSHAKE = 1;
    public static final short EXECUTEPHASE = 2;
    public static final short VERTEXVALUES = 3;
    public static final short GATHERVALUES = 4;
    public static final short NODEINFO = 5;
    public static final short FINISHED_PHASE = 6;



    public static void registerDecoder(short messageId, GraphLabMessageDecoder decoder) {
        decoders.put(messageId, decoder);
    }


    public static GraphLabMessageDecoder getDecoder(short messageId) {
        return decoders.get(messageId);
    }

}
