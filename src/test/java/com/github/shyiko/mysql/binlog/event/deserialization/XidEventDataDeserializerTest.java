package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.XidEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;

public class XidEventDataDeserializerTest {
    private static final byte[] DATA = {
        0x0f, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
    };

    @Test
    public void deserialize_shouldProperlyDeserializeData_withInputStream() throws IOException {
        XidEventDataDeserializer deserializer = new XidEventDataDeserializer();

        XidEventData actual = deserializer.deserialize(new ByteArrayInputStream(DATA));

        assertEventData(actual);
    }

    @Test
    public void deserialize_shouldProperlyDeserializeData_withBinaryReader() throws IOException{
        XidEventDataDeserializer deserializer = new XidEventDataDeserializer();

        XidEventData actual = deserializer.deserialize(new BinaryLogEventDataReader(DATA));

        assertEventData(actual);
    }

    private static void assertEventData(XidEventData actual) {
        assertEquals(1807, actual.getXid());
    }
}
