package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.FormatDescriptionEventData;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;

public class QueryEventDataDeserializerTest {
    private static final byte[] DATA = {
        0x7a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x2e, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x01, 0x20, 0x00, (byte)0xa0, 0x45, 0x00, 0x00, 0x00, 0x00, 0x06, 0x03, 0x73, 0x74, 0x64, 0x04,
        (byte)0xff, 0x00, (byte)0xff, 0x00, (byte)0xff, 0x00, 0x05, 0x06, 0x53, 0x59, 0x53, 0x54, 0x45, 0x4d,
        0x09, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x12, (byte)0xff, 0x00, 0x74, 0x70, 0x63, 0x63,
        0x32, 0x00, 0x42, 0x45, 0x47, 0x49, 0x4e
    };

    @Test
    public void deserialize_shouldProperlyDeserializeData_withInputStream() throws IOException {
        QueryEventDataDeserializer deserializer = new QueryEventDataDeserializer();

        QueryEventData actual = deserializer.deserialize(new ByteArrayInputStream(DATA));

        assertEventData(actual);
    }

    @Test
    public void deserialize_shouldProperlyDeserializeData_withBinaryReader() throws IOException{
        QueryEventDataDeserializer deserializer = new QueryEventDataDeserializer();

        QueryEventData actual = deserializer.deserialize(new BinaryLogEventDataReader(DATA));

        assertEventData(actual);
    }

    private static void assertEventData(QueryEventData actual) {
        assertEquals(122, actual.getThreadId());
        assertEquals(0, actual.getExecutionTime());
        assertEquals(0, actual.getErrorCode());
        assertEquals("tpcc2", actual.getDatabase());
        assertEquals("BEGIN", actual.getSql());
    }
}
