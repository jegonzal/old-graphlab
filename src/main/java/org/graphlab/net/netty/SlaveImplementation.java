package org.graphlab.net.netty;

import org.graphlab.net.GraphLabNode;
import org.graphlab.net.GraphLabNodeInfo;
import org.graphlab.net.netty.messages.ExecutePhaseMessage;
import org.graphlab.net.netty.messages.FinishedPhaseMessage;
import org.graphlab.net.netty.messages.HandshakeMessage;
import org.graphlab.net.netty.messages.NodeInfoMessage;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.Executors;

import static org.jboss.netty.channel.Channels.pipeline;

/**
 *
 */
public class SlaveImplementation {

    private HashMap<Integer, GraphLabNodeInfo> otherNodes = new HashMap<Integer, GraphLabNodeInfo>();
    private HashMap<Integer, Channel> nodeToNodeChannels = new HashMap<Integer, Channel>();
    private GraphLabNode graphlabNode;
    private ClientBootstrap clientBootstrap;
    private Channel masterChannel;
    private String host;
    private String masterHost;
    private int port;
    private int id;

    public SlaveImplementation(GraphLabNode graphlabNode, int id, String masterHost, String host, int port) {
        this.graphlabNode = graphlabNode;
        this.id = id;
        this.masterHost = masterHost;
        this.host = host;
        this.port = port;
    }

    public void start() {

        /* Setup channel factory and thread pools */
        ChannelFactory clientFactory =
                new NioClientSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool());
        ChannelFactory serverFactory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());

        /* Start server */
        ServerBootstrap serverBootstrap = new ServerBootstrap(serverFactory);
        serverBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = pipeline();
                pipeline.addLast("decoder", new GenericDecoder());
                pipeline.addLast("handler", new SlaveServerHandler());

                return pipeline;
            }
        });

        serverBootstrap.setOption("child.tcpNoDelay", true);
        serverBootstrap.setOption("child.keepAlive", true);

        serverBootstrap.bind(new InetSocketAddress(port));


        /* Setup connection to master */
        clientBootstrap = new ClientBootstrap(clientFactory);

        clientBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() {
                ChannelPipeline pipeline = pipeline();
                pipeline.addLast("encoder", GraphLabMessage.encoder());
                pipeline.addLast("decoder", new GenericDecoder());

                pipeline.addLast("handler", new SlaveClientHandler());
                return pipeline;
            }
        });

        clientBootstrap.setOption("tcpNoDelay", true);
        clientBootstrap.setOption("keepAlive", true);

        ChannelFuture outChannel = clientBootstrap.connect(new InetSocketAddress(masterHost, 3333));

        masterChannel = outChannel.awaitUninterruptibly().getChannel();
        // Send handshake
        masterChannel.write(new HandshakeMessage(this.id, this.host, this.port));
    }

    static {
        NodeInfoMessage.register();
        ExecutePhaseMessage.register();
    }

    class SlaveServerHandler extends SimpleChannelUpstreamHandler {

        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)  throws Exception {
            GraphLabMessage message = (GraphLabMessage) e.getMessage();

            System.out.println("Slave server handler recv: " + message);
            switch(message.getMessageId()) {
                case MessageIds.VERTEXVALUES:
                    IndexedValueArray iv = (IndexedValueArray) message;
                    graphlabNode.remoteReceiveVertexData(-1, iv.getIndices(), iv.getValues());
                    break;
                case MessageIds.GATHERVALUES:
                    IndexedValueArray gv = (IndexedValueArray) message;
                    graphlabNode.remoteReceiveGathers(-1, gv.getIndices(), gv.getValues());
                    break;
            }
        }
    }

    private void connectToSlave(final GraphLabNodeInfo nodeInfo) {
        Thread t = new Thread(new Runnable() {
            public void run() {
                ChannelFuture clientChannelFuture = clientBootstrap.connect(new InetSocketAddress(nodeInfo.getAddress(), nodeInfo.getPort()));
                Channel channel = clientChannelFuture.awaitUninterruptibly().getChannel();
                System.out.println("Connected to node " + nodeInfo);
                nodeToNodeChannels.put(nodeInfo.getId(), channel);
            }
        });
        t.start();
    }

    public void sendToNode(int nodeId, GraphLabMessage message) {
        nodeToNodeChannels.get(nodeId).write(message);
        System.out.println(id + " ==> " + nodeId + ": " + message + "; " + nodeToNodeChannels.get(nodeId));
    }

    public void sendToMaster(GraphLabMessage message) {

    }

    private class GenericDecoder extends FrameDecoder {
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

    class SlaveClientHandler extends SimpleChannelHandler {

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
            GraphLabMessage message = (GraphLabMessage) e.getMessage();

            System.out.println("Slave client handler recv: " + message);
            switch(message.getMessageId()) {
                case MessageIds.NODEINFO:
                    NodeInfoMessage nodeInfoMessage = (NodeInfoMessage) message;
                    GraphLabNodeInfo nodeInfo = nodeInfoMessage.getNodeInfo();
                    otherNodes.put(nodeInfo.getId(), nodeInfo);
                    System.out.println("Other nodes: " +  nodeInfo);
                    connectToSlave(nodeInfo);
                    break;
                case MessageIds.EXECUTEPHASE:
                    ExecutePhaseMessage execPhaseMsg = (ExecutePhaseMessage) message;
                    System.out.println("Move to phase: " + execPhaseMsg.getPhase());
                    System.out.println("From vertex: " + execPhaseMsg.getFromVertex() + " -- " + execPhaseMsg.getToVertex());
                    graphlabNode.remoteStartPhase(execPhaseMsg.getPhase(), execPhaseMsg.getFromVertex(), execPhaseMsg.getToVertex());
                    break;

            }

        }



        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
            e.getCause().printStackTrace();
            e.getChannel().close();
        }

        public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
            System.out.println("Channel connected!" + e.getChannel());

        }



        public void handleUpstream(
                ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
            if (e instanceof ChannelStateEvent) {
                System.out.println("Handle up:" + e.toString());
            }
            super.handleUpstream(ctx, e);
        }

    }


}
