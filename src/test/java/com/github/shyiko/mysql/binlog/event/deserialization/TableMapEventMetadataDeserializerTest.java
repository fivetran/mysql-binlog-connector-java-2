package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.stream.IntStream;

import org.junit.Assert;

import static junit.framework.Assert.assertEquals;

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

    @Test
    public void readBooleanList_shouldConsistentlyDeserializeBitSet() throws IOException{
        ReadBooleanListTestData[] testData = {
            // 01100101 -> 01100xxx -> {1, 2}
            new ReadBooleanListTestData(new byte[]{0x65}, 5, new int[]{1, 2}),
            // 01100101 -> 01100101 -> {1, 2, 5, 7}
            new ReadBooleanListTestData(new byte[]{0x65}, 8, new int[]{1, 2, 5, 7}),
            // 01011101 00010001 -> 01011101 000xxxxx -> {1, 3, 4, 5, 7}
            new ReadBooleanListTestData(new byte[]{0x5d, 0x11}, 11, new int[]{1, 3, 4, 5, 7}),
            // 01001000 11111111 -> 01001000 1111111x -> {1, 4, 8, 9, 10, 11, 12, 13, 14}
            new ReadBooleanListTestData(new byte[]{0x48, (byte)0xff}, 15, new int[]{1, 4, 8, 9, 10, 11, 12, 13, 14}),
            // 01010101 01101100 01000011 -> 01010101 01101100 0100001x -> {1, 3, 5, 7, 9, 10, 12, 13, 17, 22}
            new ReadBooleanListTestData(new byte[]{0x55, 0x6c, 0x43}, 23, new int[]{1, 3, 5, 7, 9, 10, 12, 13, 17, 22}),
        };

        for (int i = 0; i < testData.length; i++) {
            ReadBooleanListTestData currentTest = testData[i];
            BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(currentTest.data));
            ByteArrayInputStream inputStream = new ByteArrayInputStream(currentTest.data);

            BitSet readerBitSet = TableMapEventMetadataDeserializer.readBooleanList(reader, currentTest.length);
            BitSet streamBitSet = TableMapEventMetadataDeserializer.readBooleanList(inputStream, currentTest.length);

            BitSet expectedBitSet = IntStream.of(currentTest.expectedBitsSet).collect(BitSet::new, BitSet::set, BitSet::or);

            assertEquals(String.format("Unexpected bits set in case %d", i), expectedBitSet, readerBitSet);
            assertEquals(String.format("Reader and stream results don't match in case %d", i), streamBitSet, readerBitSet);
        }
    }

    public static class ReadBooleanListTestData {
        public final byte[] data;
        public final int length;
        public final int[] expectedBitsSet;

        public ReadBooleanListTestData(byte[] data, int length, int[] expectedBitsSet) {
            this.data = data;
            this.length = length;
            this.expectedBitsSet = expectedBitsSet;
        }
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
