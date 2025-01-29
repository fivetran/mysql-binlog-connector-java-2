package com.github.shyiko.mysql.binlog.event;

import com.github.shyiko.mysql.binlog.event.deserialization.BinaryLogEventDataReader;

import java.nio.ByteBuffer;

/**
 * The class represents a fully read binary log event that hasn't been parsed yet. Events spanning multiple
 * packets are still represented by a single {@link RawBinaryLogEvent}.
 * <p>
 * It can also represent an explicit EOF event sent by the server. Such an event doesn't have any data.
 */
public class RawBinaryLogEvent {
    private final boolean isServerEof;
    private final ByteBuffer buffer;


    public BinaryLogEventDataReader getEventDataReader() {
        return new BinaryLogEventDataReader(buffer.duplicate());
    }

    public boolean isServerEof() {
        return isServerEof;
    }

    public static RawBinaryLogEvent createServerEof() {
        return new RawBinaryLogEvent(null, true);
    }

    public static RawBinaryLogEvent createDataEvent(byte[] bytes) {
        return new RawBinaryLogEvent(bytes, false);
    }

    private RawBinaryLogEvent(byte[] bytes, boolean isServerEof) {
        this.buffer = bytes != null ? ByteBuffer.wrap(bytes) : ByteBuffer.allocate(0);
        this.isServerEof = isServerEof;
    }
}
