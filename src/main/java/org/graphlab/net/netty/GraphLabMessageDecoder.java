package org.graphlab.net.netty;

import org.jboss.netty.buffer.ChannelBuffer;

/**
 *
 */
public interface GraphLabMessageDecoder {

    GraphLabMessage decode(ChannelBuffer buffer);

}
