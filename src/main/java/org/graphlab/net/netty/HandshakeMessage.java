package org.graphlab.net.netty;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;

import static org.jboss.netty.buffer.ChannelBuffers.*;

/**
 *
 */
public class HandshakeMessage extends GraphLabMessage {
    private int nodeId;
    private String address;
    private int port;

    public HandshakeMessage(int nodeId, String address, int port) {
        super(MessageIds.HANDSHAKE);
        this.nodeId = nodeId;
        this.address = address;
        this.port = port;
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public void encode(ChannelBuffer buf) {
        byte[] addrBytes = address.getBytes();
        buf.writeInt(nodeId);
        buf.writeInt(addrBytes.length);
        buf.writeBytes(addrBytes);
        buf.writeInt(port);
    }

}
