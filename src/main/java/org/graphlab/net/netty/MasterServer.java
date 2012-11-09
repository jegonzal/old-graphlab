package org.graphlab.net.netty;

import org.graphlab.net.GraphLabNodeInfo;
import org.graphlab.net.Master;
import org.graphlab.net.netty.messages.FinishedPhaseMessage;
import org.graphlab.net.netty.messages.HandshakeMessage;
import org.graphlab.net.netty.messages.NodeInfoMessage;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

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
        FinishedPhaseMessage.register();
    }

    private Map<Integer, Integer> channelIdToNodeId =
            Collections.synchronizedMap(new HashMap<Integer, Integer>());
    private Map<Integer, GraphLabNodeInfo> nodes =
            Collections.synchronizedMap(new HashMap<Integer, GraphLabNodeInfo>());
    private Map<Integer, Channel> nodesToChannels =
            Collections.synchronizedMap(new HashMap<Integer, Channel>());

    private Master master;

    public MasterServer(Master master) {
        this.master = master;
    }


    private class MasterDecoder extends FrameDecoder {
        @Override
        protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buf) throws Exception {
            buf.markReaderIndex();
            short messageId = buf.readShort();
            Object message = MessageIds.getDecoder(messageId).decode(ctx, channel, buf);
            if (message == null) {
                buf.resetReaderIndex();
            }
            return message;
        }
    }

    public void start() {
        ChannelFactory factory =  new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());
        ServerBootstrap bootstrap = new ServerBootstrap(factory);
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = pipeline();
                pipeline.addLast("encoder", GraphLabMessage.encoder());
                pipeline.addLast("decoder", new MasterDecoder());
                pipeline.addLast("handler", new MasterServerHandler());

                return pipeline;
            }
        });

        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);

        bootstrap.bind(new InetSocketAddress(3333));
    }

    public int getNumOfRegisteredNodes() {
        return nodes.size();
    }

    public void sendClient(int nodeId, GraphLabMessage message) {
        Channel channel = nodesToChannels.get(nodeId);
        System.out.println("Send to " + nodeId + " , message=" + message);
        if (channel != null) {
            channel.write(message);
        }  else {
            throw new IllegalStateException("No channel for node: " + nodeId);
        }
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




        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)  throws Exception {
            System.out.println("Master message received: " + e.getMessage());
            GraphLabMessage message = (GraphLabMessage) e.getMessage();

                switch(message.getMessageId()) {
                    case MessageIds.HANDSHAKE:
                        HandshakeMessage handshake = (HandshakeMessage) message;

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
                        GraphLabNodeInfo nodeInfo =  new GraphLabNodeInfo(handshake.getNodeId(),
                                InetAddress.getByName(handshake.getAddress()),
                                handshake.getPort());
                        synchronized (this) {
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
                        master.remoteRegisterSlave(nodeInfo);
                        System.out.println("Nodes now: " + nodes);
                        break;
                    case MessageIds.FINISHED_PHASE:
                        FinishedPhaseMessage finishedPhaseMessage = (FinishedPhaseMessage) message;
                        int nodeId = channelIdToNodeId.get(e.getChannel().getId()); // Can we store it in the context?
                        System.out.println("Finish: " + finishedPhaseMessage + " , master=" + master);
                        master.remoteFinishedPhase(nodeId, finishedPhaseMessage.getPhase());
                        break;
                }
        }

    }

}
