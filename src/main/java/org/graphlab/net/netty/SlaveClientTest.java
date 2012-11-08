package org.graphlab.net.netty;

import org.graphlab.net.netty.messages.HandshakeMessage;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.concurrent.Executors;

import static org.jboss.netty.channel.Channels.*;


/**
 *
 */
public class SlaveClientTest {

    static class SlaveClientHandler extends SimpleChannelHandler {

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
            ChannelBuffer buf = (ChannelBuffer) e.getMessage();
            long currentTimeMillis = buf.readInt() * 1000L;
            System.out.println(new Date(currentTimeMillis));
            e.getChannel().close();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
            e.getCause().printStackTrace();
            e.getChannel().close();
        }

        public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
            System.out.println("Channel connected!" + e.getChannel());
            try {
                e.getChannel().write(new HandshakeMessage(99, InetAddress.getLocalHost().getHostName(), 245));
            } catch (Exception err) {
                err.printStackTrace();
            }
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

        ChannelFactory factory =
                new NioClientSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool());

        ClientBootstrap bootstrap = new ClientBootstrap(factory);

        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() {
                ChannelPipeline pipeline = pipeline();
                pipeline.addLast("encoder", GraphLabMessage.encoder());
                pipeline.addLast("handler", new SlaveClientHandler());
                return pipeline;
            }
        });

        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);

        ChannelFuture outChannel = bootstrap.connect(new InetSocketAddress(host, port));

        Channel channel = outChannel.awaitUninterruptibly().getChannel();

        System.out.println("DONE");
    }
}
