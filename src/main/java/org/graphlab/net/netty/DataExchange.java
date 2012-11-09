package org.graphlab.net.netty;

import edu.cmu.graphchi.datablocks.*;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;

/**
 */

public class DataExchange<VertexType, GatherType> {

    private BytesToValueConverter<VertexType> vertexValueConverter;
    private BytesToValueConverter<GatherType> gatherValueConverter;


    public DataExchange(BytesToValueConverter<VertexType> vertexValueConverter, BytesToValueConverter<GatherType> gatherValueConverter) {
        this.vertexValueConverter = vertexValueConverter;
        this.gatherValueConverter = gatherValueConverter;

        registerDecoders();
    }

    public void registerDecoders() {
        MessageIds.registerDecoder(MessageIds.VERTEXVALUES, getDecoder(MessageIds.VERTEXVALUES, vertexValueConverter));
        MessageIds.registerDecoder(MessageIds.GATHERVALUES, getDecoder(MessageIds.GATHERVALUES, gatherValueConverter));
        MessageIds.registerDecoder(MessageIds.TOPRESULTS, getDecoder(MessageIds.TOPRESULTS, gatherValueConverter));

    }

    private <T> GraphLabMessageDecoder getDecoder(final short messageId, final BytesToValueConverter<T> converter) {
      return new GraphLabMessageDecoder() {
            @Override
            public Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) {
                if (buffer.readableBytes() < 4) {
                    return null;
                }

                int n = buffer.readInt();
                int sizeOf = converter.sizeOf();

                if (buffer.readableBytes() < n * (4 + sizeOf)) {
                    return null;
                }

                int[] vertexIds = new int[n];
                T[] values = (T[]) new Object[n];
                byte[] template = new byte[sizeOf];

                for(int i=0; i<n; i++) {

                    vertexIds[i] = buffer.readInt();
                    buffer.readBytes(template);
                    values[i] = converter.getValue(template);

                }
                return new IndexedValueArray<T>(messageId, vertexIds, values);
            }
        };
    }

    public GraphLabMessage getMessageForVertexArray(final int[] vertexIds,  final VertexType[] values) {
        return getMessageForValueArray(MessageIds.VERTEXVALUES, vertexIds, values, vertexValueConverter);
    }

    public GraphLabMessage getMessageForGatherArray(final int[] vertexIds, final GatherType[] values) {
        return getMessageForValueArray(MessageIds.GATHERVALUES, vertexIds, values, gatherValueConverter);
    }

    public GraphLabMessage getMessageForTopQueryResult(final int[] vertexIds,  final VertexType[] values) {
        return getMessageForValueArray(MessageIds.TOPRESULTS, vertexIds, values, vertexValueConverter);
    }

    private <T> GraphLabMessage getMessageForValueArray(final short msgId, final int[] vertexIds,  final T[] values,
                                                        final BytesToValueConverter<T> valueConverter) {
        if (vertexIds.length != values.length)
            throw new IllegalArgumentException("Vertex id value length differ! " + vertexIds.length + " != "
                + values.length);

        return new GraphLabMessage(msgId) {
            @Override
            public void encode(ChannelBuffer buf) {
                buf.writeInt(vertexIds.length);
                int sizeOf = valueConverter.sizeOf();
                byte[] data = new byte[vertexIds.length * (4 + sizeOf)];

                IntConverter intConverter = new IntConverter();
                byte[] vidData = new byte[4];
                byte[] valData = new byte[sizeOf];
                int offset = 0;
                for(int i=0; i < vertexIds.length; i++) {
                    int vid = vertexIds[i];
                    T val = values[i];
                    intConverter.setValue(vidData, vid);
                    valueConverter.setValue(valData, val);

                    System.arraycopy(vidData, 0, data, offset, 4);
                    System.arraycopy(valData, 0, data, offset + 4, sizeOf);
                    offset += 4 + sizeOf;
                }

                while (!buf.writable()) {
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("Not writable...");
                }
                buf.writeBytes(data);
            }
        };

    }
}
