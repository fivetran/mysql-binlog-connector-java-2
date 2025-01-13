package com.github.shyiko.mysql.binlog.event;

import com.github.shyiko.mysql.binlog.event.deserialization.BinaryLogEventDataReader;

import java.nio.ByteBuffer;

/**
 * The class represents a fully read binary log event that hasn't been parsed yet. Events spanning multiple
 * packets are still represented by a single {@link RawBinaryLogEvent}.
 */
public class RawBinaryLogEvent {
    private final ByteBuffer buffer;

    /**
     * Creates a new raw event with data present in the buffer.
     */
    public RawBinaryLogEvent(byte[] bytes) {
        this.buffer = ByteBuffer.wrap(bytes);
    }


    public BinaryLogEventDataReader getEventDataReader() {
        return new BinaryLogEventDataReader(buffer.duplicate());
    }
}
