package org.graphlab.net.netty;


import org.jboss.netty.buffer.ChannelBuffer;

public class IndexedValueArray<T> extends GraphLabMessage {

    private int[] indices;
    private T[] values;



    // Ugly
    public IndexedValueArray(short messageId, int[] indices, T[] values) {
        super(messageId);
        this.indices = indices;
        this.values = values;
    }

    public int[] getIndices() {
        return indices;
    }

    public T[] getValues() {
        return values;
    }

    @Override
    public void encode(ChannelBuffer buf) {
        throw new RuntimeException("Should never be called");
    }
}
