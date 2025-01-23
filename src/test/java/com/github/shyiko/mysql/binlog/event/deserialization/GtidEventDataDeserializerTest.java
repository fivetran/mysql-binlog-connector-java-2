package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.FormatDescriptionEventData;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;

public class GtidEventDataDeserializerTest {
    private static final byte[] DATA = {
        0x00, (byte)0xa5, 0x77, 0x2f, (byte)0xd5, (byte)0xd7, 0x12, 0x11, (byte)0xef, (byte)0x86,
        0x05, 0x42, 0x01, 0x0a, 0x40, 0x00, 0x29, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x74, (byte)0xc3, 0x2b, 0x44, 0x20, 0x2c, 0x06, (byte)0xfc, (byte)0xce, 0x13,
        0x13, 0x3a, 0x01, 0x00, 0x29, 0x55, (byte)0x9e, 0x22
    };

    @Test
    public void deserialize_shouldProperlyDeserializeData_withInputStream() throws IOException {
        GtidEventDataDeserializer deserializer = new GtidEventDataDeserializer();

        GtidEventData actual = deserializer.deserialize(new ByteArrayInputStream(DATA));

        assertEventData(actual);
    }

    @Test
    public void deserialize_shouldProperlyDeserializeData_withBinaryReader() throws IOException{
        GtidEventDataDeserializer deserializer = new GtidEventDataDeserializer();

        GtidEventData actual = deserializer.deserialize(new BinaryLogEventDataReader(DATA));

        assertEventData(actual);
    }

    private static void assertEventData(GtidEventData actual) {
        assertEquals("a5772fd5-d712-11ef-8605-42010a400029:3", actual.getGtid());
        assertEquals(0, actual.getFlags());
        assertEquals(0, actual.getLastCommitted());
        assertEquals(1, actual.getSequenceNumber());
        assertEquals(1737366954558324L, actual.getImmediateCommitTimestamp());
        assertEquals(1737366954558324L, actual.getOriginalCommitTimestamp());
        assertEquals(80403, actual.getImmediateServerVersion());
        assertEquals(80403, actual.getOriginalServerVersion());
        assertEquals(5070, actual.getTransactionLength());
    }
}
