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

    public SlaveImplementation(int id, String masterHost, String host, int port) {
        // this.graphlabNode = graphlabNode;
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
            ChannelBuffer buf = (ChannelBuffer) e.getMessage();
            while(buf.readable()) {
                short message = buf.readShort();
                System.out.println("Node-2-Node, received: " + message);
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
    }

    public void sendToMaster(GraphLabMessage message) {

    }

    class SlaveClientHandler extends SimpleChannelHandler {

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
            ChannelBuffer buf = (ChannelBuffer) e.getMessage();
            while(buf.readable()) {
                short message = buf.readShort();
                System.out.println("Slave client handler recv: " + message);
                switch(message) {
                    case MessageIds.NODEINFO:
                        NodeInfoMessage nodeInfoMessage = (NodeInfoMessage)
                                MessageIds.getDecoder(message).decode(buf);
                        GraphLabNodeInfo nodeInfo = nodeInfoMessage.getNodeInfo();
                        otherNodes.put(nodeInfo.getId(), nodeInfo);
                        System.out.println("Other nodes: " +  nodeInfo);
                        connectToSlave(nodeInfo);
                        break;
                    case MessageIds.EXECUTEPHASE:
                        ExecutePhaseMessage execPhaseMsg = (ExecutePhaseMessage) MessageIds.getDecoder(message).decode(buf);
                        System.out.println("Move to phase: " + execPhaseMsg.getPhase());
                        System.out.println("From vertex: " + execPhaseMsg.getFromVertex() + " -- " + execPhaseMsg.getToVertex());
                        try {
                            Thread.sleep(500l);
                        } catch (InterruptedException e1) { }

                        masterChannel.write(new FinishedPhaseMessage(execPhaseMsg.getPhase(),
                                execPhaseMsg.getFromVertex(), execPhaseMsg.getToVertex()));
                        break;
                }

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


    public static void main(String[] args) {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        int nodeId = Integer.parseInt(args[2]);

        for(int i=0; i<4; i++) {
            SlaveImplementation slave = new SlaveImplementation(nodeId + i, host, host, port + i);
            slave.start();
        }
        System.out.println("DONE");
    }
}
