package org.graphlab.net.netty;

import org.graphlab.net.GraphLabNodeInfo;
import org.graphlab.net.netty.messages.HandshakeMessage;
import org.graphlab.net.netty.messages.NodeInfoMessage;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.jboss.netty.channel.Channels.*;

/**
 *
 */
public class MasterServer{

    static {
        HandshakeMessage.register();
    }

    private Map<Integer, Integer> channelIdToNodeId =
            Collections.synchronizedMap(new HashMap<Integer, Integer>());
    private Map<Integer, GraphLabNodeInfo> nodes =
            Collections.synchronizedMap(new HashMap<Integer, GraphLabNodeInfo>());
    private Map<Integer, Channel> nodesToChannels =
            Collections.synchronizedMap(new HashMap<Integer, Channel>());

    public MasterServer() {}

    public void start() {
        ChannelFactory factory =  new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());
        ServerBootstrap bootstrap = new ServerBootstrap(factory);
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = pipeline();
                pipeline.addLast("handler", new MasterServerHandler());
                pipeline.addLast("encoder", GraphLabMessage.encoder());

                return pipeline;
            }
        });

        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);

        bootstrap.bind(new InetSocketAddress(3333));
    }


    private class MasterServerHandler  extends SimpleChannelHandler {
        public void handleUpstream(
                ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
            if (e instanceof ChannelStateEvent) {
                System.out.println("Handle upstream: " + e.toString());
            }
            super.handleUpstream(ctx, e);
        }

        @Override
        public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
            int channelId = e.getChannel().getId();
            if (channelIdToNodeId.containsKey(channelId)) {
                int nodeId = channelIdToNodeId.get(channelId);
                nodes.remove(nodeId);
                nodesToChannels.remove(nodeId);
                channelIdToNodeId.remove(channelId);
            }
            System.out.println("Disconnected by " + channelId + ": nodes now: " + nodes);
        }

        protected void sendClient(int nodeId, GraphLabMessage message) {
            Channel channel = nodesToChannels.get(nodeId);
            System.out.println("Send to " + nodeId + " , message=" + message);
            if (channel != null) {
                channel.write(message);
            }  else {
                throw new IllegalStateException("No channel for node: " + nodeId);
            }
        }


        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)  throws Exception {
            ChannelBuffer buf = (ChannelBuffer) e.getMessage();
            while(buf.readable()) {
                short message = buf.readShort();

                System.out.println("Received message:" + message);
                System.out.flush();

                switch(message) {
                    case MessageIds.HANDSHAKE:
                        HandshakeMessage handshake = (HandshakeMessage)
                                MessageIds.getDecoder(MessageIds.HANDSHAKE).decode(buf);

                        // Must be faster way to do this..
                        if (channelIdToNodeId.values().contains(handshake.getNodeId())) {
                            System.out.println("Warning: already had node id, replacing");
                            Integer keyToRemove = null;
                            for(Map.Entry<Integer, Integer> entry : channelIdToNodeId.entrySet()) {
                                if (entry.getValue() == handshake.getNodeId()) {
                                    keyToRemove = entry.getKey();
                                    break;
                                }
                            }
                            if (keyToRemove != null) channelIdToNodeId.remove(keyToRemove);
                        }

                        // Register
                        synchronized (this) {

                            GraphLabNodeInfo nodeInfo =  new GraphLabNodeInfo(handshake.getNodeId(),
                                    InetAddress.getByName(handshake.getAddress()),
                                    handshake.getPort());
                            nodes.put(handshake.getNodeId(),
                                    nodeInfo);
                            channelIdToNodeId.put(e.getChannel().getId(), handshake.getNodeId());
                            nodesToChannels.put(handshake.getNodeId(), ctx.getChannel());

                            // Send all other nodes this node
                            for(Integer nodeId: nodesToChannels.keySet()) {
                                System.out.println("*" + nodeId + "; " + handshake.getNodeId());
                                if (nodeId != handshake.getNodeId()) {
                                    sendClient(nodeId, new NodeInfoMessage(nodeInfo));
                                    // Send other clients to this node
                                    sendClient(nodeInfo.getId(), new NodeInfoMessage(nodes.get(nodeId)));
                                }
                            }


                        }

                        System.out.println("Nodes now: " + nodes);
                        break;
                }
            }
        }

    }

    public static void main(String[] args) {
        MasterServer master = new MasterServer();
        master.start();
    }
}
