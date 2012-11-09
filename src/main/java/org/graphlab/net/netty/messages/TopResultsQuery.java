package org.graphlab.net.netty.messages;

import org.graphlab.net.netty.GraphLabMessage;
import org.graphlab.net.netty.GraphLabMessageDecoder;
import org.graphlab.net.netty.MessageIds;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;

/**
 *
 */
public class TopResultsQuery extends GraphLabMessage {

    public static void registerDecoder() {
        MessageIds.registerDecoder(MessageIds.TOPQUERY, new GraphLabMessageDecoder() {
            @Override
            protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buf) {
                if (buf.readableBytes() < 4) return null;
                int topN = buf.readInt();
                return new TopResultsQuery(topN);
            }
        });
    }

    int topN;

    public TopResultsQuery(int topN) {
        super(MessageIds.TOPQUERY);
        this.topN = topN;
    }

    public int getTopN() {
        return topN;
    }

    @Override
    public void encode(ChannelBuffer buf) {
        buf.writeInt(topN);
    }
}
