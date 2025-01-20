package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import org.junit.Test;

import java.io.IOException;
import java.util.BitSet;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.Assert;

public class TableMapEventMetadataDeserializerTest {
    private static final byte[] DATA = {
        (byte)0x86, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x05, 0x74, 0x70, 0x63, 0x63, 0x32, 0x00, 0x09,
        0x77, 0x61, 0x72, 0x65, 0x68, 0x6f, 0x75, 0x73, 0x65, 0x00, 0x09, 0x03, (byte)0xf6, (byte)0xf6, 0x0f,
        0x0f, 0x0f, 0x0f, (byte)0xfe, (byte)0xfe, 0x10, 0x0c, 0x02, 0x04, 0x04, 0x0a, 0x00, 0x14, 0x00, 0x14,
        0x00, 0x14, 0x00, (byte)0xfe, 0x02, (byte)0xfe, 0x09, (byte)0xfe, 0x01, 0x01, 0x01, 0x00, 0x02, 0x01,
        0x2f
    };

    @org.junit.Test
    public void deserialize_shouldProperlyDeserializeData_withInputStream() throws IOException {
        TableMapEventDataDeserializer deserializer = new TableMapEventDataDeserializer();

        TableMapEventData actual = deserializer.deserialize(new ByteArrayInputStream(DATA));

        assertEventData(actual);
    }

    @Test
    public void deserialize_shouldProperlyDeserializeData_withBinaryReader() throws IOException{
        TableMapEventDataDeserializer deserializer = new TableMapEventDataDeserializer();

        TableMapEventData actual = deserializer.deserialize(new BinaryLogEventDataReader(DATA));

        assertEventData(actual);
    }

    private static void assertEventData(TableMapEventData actual) {
        Assert.assertEquals(134L, actual.getTableId());
        Assert.assertEquals("tpcc2", actual.getDatabase());
        Assert.assertEquals("warehouse", actual.getTable());
        Assert.assertArrayEquals(
            new byte[]{0x03, (byte)0xf6, (byte)0xf6, 0x0f, 0x0f, 0x0f, 0x0f, (byte)0xfe, (byte)0xfe},
            actual.getColumnTypes());
        Assert.assertArrayEquals(new int[]{0, 524, 1028, 10, 20, 20, 20, 65026, 65033}, actual.getColumnMetadata());

        BitSet expectedBitSet = IntStream.range(1, 9).collect(BitSet::new, BitSet::set, BitSet::or);
        Assert.assertEquals(expectedBitSet, actual.getColumnNullability());

        TableMapEventMetadata eventMetadata = actual.getEventMetadata();
        Assert.assertEquals(new BitSet(), eventMetadata.getSignedness());
        Assert.assertEquals(47, eventMetadata.getDefaultCharset().getDefaultCharsetCollation());
    }
}
