package org.graphlab.net.netty.messages;

import org.graphlab.net.GraphLabNodeInfo;
import org.graphlab.net.netty.GraphLabMessage;
import org.graphlab.net.netty.GraphLabMessageDecoder;
import org.graphlab.net.netty.MessageIds;
import org.jboss.netty.buffer.ChannelBuffer;

import java.net.InetAddress;

/**
 * practically duplicate of HandshakeMessage...
 */
public class NodeInfoMessage extends GraphLabMessage {

    private GraphLabNodeInfo nodeInfo;

    public static void register() {
        MessageIds.registerDecoder(MessageIds.NODEINFO, new GraphLabMessageDecoder() {
            @Override
            public GraphLabMessage decode(ChannelBuffer buf) {
                int nodeId = buf.readInt();
                String address = GraphLabMessage.readString(buf);
                int port = buf.readInt();
                try {
                    return new NodeInfoMessage(new GraphLabNodeInfo(nodeId, InetAddress.getByName(address), port));
                } catch (Exception err) {
                    throw new RuntimeException(err);
                }
            }
        });
    }
    static {
        register();
    }

    public NodeInfoMessage(GraphLabNodeInfo nodeInfo) {
        super(MessageIds.NODEINFO);
        this.nodeInfo = nodeInfo;
    }

    public GraphLabNodeInfo getNodeInfo() {
        return nodeInfo;
    }

    @Override
    public void encode(ChannelBuffer buf) {
        byte[] addrBytes = nodeInfo.getAddress().getHostAddress().getBytes();
        buf.writeInt(nodeInfo.getId());
        buf.writeInt(addrBytes.length);
        buf.writeBytes(addrBytes);
        buf.writeInt(nodeInfo.getPort());
    }
}
