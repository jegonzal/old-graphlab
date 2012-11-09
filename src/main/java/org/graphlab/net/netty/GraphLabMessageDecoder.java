package org.graphlab.net.netty;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

/**
 *
 */
public abstract class GraphLabMessageDecoder extends FrameDecoder {


    @Override
    protected abstract Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buf);

}
