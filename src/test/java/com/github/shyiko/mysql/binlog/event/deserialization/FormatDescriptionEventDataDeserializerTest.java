package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.FormatDescriptionEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import org.junit.Test;
import static junit.framework.Assert.assertEquals;

import java.io.IOException;

public class FormatDescriptionEventDataDeserializerTest {
    private static final byte[] DATA = {
        0x04, 0x00, 0x38, 0x2e, 0x34, 0x2e, 0x33, 0x2d, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x13, 0x00, 0x0d, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x04, 0x00, 0x00,
        0x00, 0x63, 0x00, 0x04, 0x1a, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
        0x00, 0x00, 0x0a, 0x0a, 0x0a, 0x2a, 0x2a, 0x00, 0x12, 0x34, 0x00, 0x0a, 0x28, 0x00,
        0x00, 0x01, (byte)0xfd, 0x76, 0x36, 0x2d
        };

    @Test
    public void deserialize_shouldProperlyDeserializeData_withInputStream() throws IOException {
        FormatDescriptionEventDataDeserializer deserializer = new FormatDescriptionEventDataDeserializer();

        FormatDescriptionEventData actual = deserializer.deserialize(new ByteArrayInputStream(DATA));

        assertEventData(actual);
    }

    @Test
    public void deserialize_shouldProperlyDeserializeData_withBinaryReader() throws IOException{
        FormatDescriptionEventDataDeserializer deserializer = new FormatDescriptionEventDataDeserializer();

        FormatDescriptionEventData actual = deserializer.deserialize(new BinaryLogEventDataReader(DATA));

        assertEventData(actual);
    }

    private static void assertEventData(FormatDescriptionEventData actual) {
        assertEquals(4, actual.getBinlogVersion());
        assertEquals("8.4.3-google", actual.getServerVersion());
        assertEquals(19, actual.getHeaderLength());
        assertEquals(99, actual.getDataLength());
        assertEquals(ChecksumType.CRC32, actual.getChecksumType());
    }
}
