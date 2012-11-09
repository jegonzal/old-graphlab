package org.graphlab.net.netty.messages;

import org.graphlab.ExecutionPhase;
import org.graphlab.net.netty.GraphLabMessage;
import org.graphlab.net.netty.GraphLabMessageDecoder;
import org.graphlab.net.netty.MessageIds;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 */
public class FinishedPhaseMessage extends  GraphLabMessage {
    private ExecutionPhase phase;
    private int fromVertex;
    private int toVertex;



    public static void register() {
        MessageIds.registerDecoder(MessageIds.FINISHED_PHASE, new GraphLabMessageDecoder() {
            @Override
            public GraphLabMessage decode(ChannelBuffer buf) {
                ExecutionPhase phase = new ExecutionPhase(buf.readInt());
                int fromVertex = buf.readInt();
                int toVertex = buf.readInt();
                return new FinishedPhaseMessage(phase, fromVertex, toVertex);
            }
        });
    }
    static {
        register();
    }

    public FinishedPhaseMessage(ExecutionPhase phase, int fromVertex, int toVertex) {
        super(MessageIds.FINISHED_PHASE);
        this.phase = phase;
        this.fromVertex = fromVertex;
        this.toVertex = toVertex;
    }

    public ExecutionPhase getPhase() {
        return phase;
    }

    public void setPhase(ExecutionPhase phase) {
        this.phase = phase;
    }

    public int getFromVertex() {
        return fromVertex;
    }

    public void setFromVertex(int fromVertex) {
        this.fromVertex = fromVertex;
    }

    public int getToVertex() {
        return toVertex;
    }

    public void setToVertex(int toVertex) {
        this.toVertex = toVertex;
    }

    @Override
    public void encode(ChannelBuffer buf) {
        buf.writeInt(phase.getPhaseNum());
        buf.writeInt(fromVertex);
        buf.writeInt(toVertex);
    }

}
