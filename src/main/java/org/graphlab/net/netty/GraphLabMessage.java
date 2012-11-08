package org.graphlab.net.netty;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

/**
 */
public abstract class GraphLabMessage {

    private short messageId;

    protected GraphLabMessage(short messageId) {
        this.messageId = messageId;
    }

    public abstract void encode(ChannelBuffer buf);

    public static OneToOneEncoder encoder() {
        return new OneToOneEncoder () {

            protected Object encode(
                    ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
                GraphLabMessage glMessage = (GraphLabMessage) msg;

                ChannelBuffer buf = ChannelBuffers.dynamicBuffer();   // TODO, ask the object for size
                buf.writeShort(glMessage.messageId);
                glMessage.encode(buf);
                return buf;
            }
        };
    }

}
