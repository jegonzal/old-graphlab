package org.graphlab.net.netty;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import static org.jboss.netty.channel.Channels.*;

/**
 *
 */
public class MasterServer extends SimpleChannelUpstreamHandler {


    public void handleUpstream(
            ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        if (e instanceof ChannelStateEvent) {
            System.out.println("Handle upstream: " + e.toString());
        }
        super.handleUpstream(ctx, e);
    }


    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)  throws Exception {
        ChannelBuffer buf = (ChannelBuffer) e.getMessage();
        while(buf.readable()) {
            short message = buf.readShort();

            System.out.println("Received message:" + message);
            System.out.flush();

            switch(message) {
                case MessageIds.HANDSHAKE:
                    int nodeId = buf.readInt();
                    String address = readString(buf);
                    int port = buf.readInt();
                    System.out.println("Handshake from: " + address + ":" + port);
            }
        }
    }

    private String readString(ChannelBuffer buf) {
        int numBytes = buf.readInt();
        byte[] bytes = new byte[numBytes];
        buf.readBytes(bytes);
        return new String(bytes);
    }


    public static void main(String[] args) {
        ChannelFactory factory =  new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());
        ServerBootstrap bootstrap = new ServerBootstrap(factory);
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = pipeline();
                pipeline.addLast("handler", new MasterServer());
                return pipeline;
            }
        });

        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);

        bootstrap.bind(new InetSocketAddress(3333));
    }
}
