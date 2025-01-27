package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.stream.IntStream;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

public class BinaryLogEventDataReaderTest {

    @Test
    public void readLongWithSkip_shouldProperlyReadAndSkipData() {
        byte[] data = {0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, (byte)0x88, (byte)0x99, (byte)0xaa};
        BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(data));

        long value = reader.readLongWithSkip(2);

        assertEquals(0x665544332211L, value);
        assertEquals(2, reader.available());
    }

    @Test
    public void readLong_shouldProperlyReadLittleEndianLongs() {
        byte[] data = {(byte)0xaa, (byte)0x99, (byte)0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11 };
        long[] expectedValues = {
            0xaaL, 0x99aaL, 0x8899aaL, 0x778899aaL, 0x66778899aaL, 0x5566778899aaL, 0x445566778899aaL, 0x33445566778899aaL
        };

        for (int i = 1; i <= 8; i++) {
            BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(data));
            long value = reader.readLong(i);
            assertEquals(String.format("Error parsing %d-byte(s) long", i), expectedValues[i-1], value);
            assertEquals(data.length - i, reader.available());
        }
    }

    @Test
    public void readLongBE_shouldProperlyReadBigEndianLongs() {
        byte[] data = {0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, (byte)0x88, (byte)0x99, (byte)0xaa};
        long[] expectedValues = {
            0x11L, 0x1122L, 0x112233L, 0x11223344L, 0x1122334455L, 0x112233445566L, 0x11223344556677L, 0x1122334455667788L
        };

        for (int i = 1; i <= 8; i++) {
            BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(data));
            long value = reader.readLongBE(i);
            assertEquals(String.format("Error parsing %d-byte(s) long", i), expectedValues[i-1], value);
            assertEquals(data.length - i, reader.available());
        }
    }

    @Test
    public void readInteger_shouldProperlyReadLittleEndianIntegers() {
        byte[] data = {(byte)0xaa, (byte)0x99, (byte)0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11 };
        long[] expectedValues = {
            0xaaL, 0x99aaL, 0x8899aaL, 0x778899aaL
        };

        for (int i = 1; i <= 4; i++) {
            BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(data));
            long value = reader.readInteger(i);
            assertEquals(String.format("Error parsing %d-byte(s) int", i), expectedValues[i-1], value);
            assertEquals(data.length - i, reader.available());
        }
    }

    @Test
    public void readIntegerBE_shouldProperlyReadBigEndianIntegers() {
        byte[] data = {0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, (byte)0x88, (byte)0x99, (byte)0xaa};
        long[] expectedValues = {
            0x11L, 0x1122L, 0x112233L, 0x11223344L
        };

        for (int i = 1; i <= 4; i++) {
            BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(data));
            long value = reader.readIntegerBE(i);
            assertEquals(String.format("Error parsing %d-byte(s) int", i), expectedValues[i-1], value);
            assertEquals(data.length - i, reader.available());
        }
    }

    @Test
    public void readUnsignedByte_shouldProperlyHandlePositiveByte() {
        byte[] data = {0x11, 0x22, 0x33};
        BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(data));

        int value = reader.readUnsignedByte();

        assertEquals(0x11, value);
        assertEquals(2, reader.available());
    }

    @Test
    public void readUnsignedByte_shouldProperlyHandleNegativeByte() {
        byte[] data = {(byte)0xaa, 0x11, (byte)0xcc};
        BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(data));

        int value = reader.readUnsignedByte();

        assertEquals(0xaa, value);
        assertEquals(2, reader.available());
    }

    @Test
    public void peekUnsignedByte_shouldProperlyHandlePositiveByte() {
        byte[] data = {0x11, 0x22, 0x33};
        BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(data));

        int value = reader.peekUnsignedByte();

        assertEquals(0x11, value);
        assertEquals(3, reader.available());
    }

    @Test
    public void peekUnsignedByte_shouldProperlyHandleNegativeByte() {
        byte[] data = {(byte)0xaa, 0x11, (byte)0xcc};
        BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(data));

        int value = reader.peekUnsignedByte();

        assertEquals(0xaa, value);
        assertEquals(3, reader.available());
    }

    @Test
    public void peekUnsignedByte_shouldReturnMinusOneIfNoBytesLeft() {
        byte[] data = {(byte)0xaa, 0x11, (byte)0xcc};
        BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(data));
        reader.skip(3);

        int value = reader.peekUnsignedByte();

        assertEquals(-1, value);
        assertEquals(0, reader.available());
    }

    @Test
    public void readByte_shouldProperlyHandlePositiveByte() {
        byte[] data = {0x11, 0x22, 0x33};
        BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(data));

        byte value = reader.readByte();

        assertEquals(17, value);
        assertEquals(2, reader.available());
    }

    @Test
    public void readByte_shouldProperlyHandleNegativeByte() {
        byte[] data = {(byte)0xaa, 0x11, (byte)0xcc};
        BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(data));

        byte value = reader.readByte();

        assertEquals(-86, value);
        assertEquals(2, reader.available());
    }

    @Test
    public void readBytes_shouldProperlyReadRequestedNumberOfBytes() {
        byte[] data = {0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, (byte)0x88, (byte)0x99, (byte)0xaa};
        BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(data));
        final int length = 7;

        byte[] bytesRead = reader.readBytes(length);

        byte[] expectedBytes = {0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77};
        assertArrayEquals(expectedBytes, bytesRead);
        assertEquals(data.length - length, reader.available());
    }

    @Test
    public void skip_shouldProperlySkipRequestedNumberOfBytes() {
        byte[] data = {0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, (byte)0x88, (byte)0x99, (byte)0xaa};
        BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(data));
        final int length = 7;

        reader.skip(length);

        assertEquals(data.length - length, reader.available());
        assertEquals(0x88, reader.peekUnsignedByte());
    }

    @Test
    public void readPackedLong_shouldProperlyHandlePackedLongValues() throws IOException {
        PackedNumberTestData[] testData = {
            new PackedNumberTestData(new byte[]{0x0a, 0x02}, 0x0aL, 1),
            new PackedNumberTestData(new byte[]{(byte)0xfa, (byte)0xa2, 0x12, 0x45, 0x11, (byte)0xd1, 0x11, 0x22, 0x33, 0x44}, 0xfaL, 9),
            new PackedNumberTestData(new byte[]{(byte)0xfc, (byte)0xa2, 0x12, 0x45, 0x11, (byte)0xd1, 0x11, 0x22, 0x33, 0x44}, 0x12a2L, 7),
            new PackedNumberTestData(new byte[]{(byte)0xfd, (byte)0xa2, 0x12, 0x45, 0x11, (byte)0xd1, 0x11, 0x22, 0x33, 0x44}, 0x4512a2L, 6),
            new PackedNumberTestData(new byte[]{(byte)0xfe, (byte)0xa2, 0x12, 0x45, 0x11, (byte)0xd1, 0x11, 0x22, 0x33, 0x44}, 0x332211d1114512a2L, 1),
            };

        for (int i = 0; i < testData.length; i++) {
            PackedNumberTestData currentTest = testData[i];
            BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(currentTest.inputBytes));

            long value = reader.readPackedLong();

            assertEquals(String.format("Unexpected packed long in case %d", i), currentTest.expectedValue, value);
            assertEquals(String.format("Unexpected bytes left in case %d", i), currentTest.expectedAvailableBytes, reader.available());
        }
    }

    @Test
    public void readPackedInteger_shouldProperlyHandlePackedIntegerValues() throws IOException {
        PackedNumberTestData[] testData = {
            new PackedNumberTestData(new byte[]{0x0a, 0x02}, 0x0aL, 1),
            new PackedNumberTestData(new byte[]{(byte)0xfa, (byte)0xa2, 0x12, 0x45, 0x11, (byte)0xd1, 0x11, 0x22, 0x33, 0x44}, 0xfaL, 9),
            new PackedNumberTestData(new byte[]{(byte)0xfc, (byte)0xa2, 0x12, 0x45, 0x11, (byte)0xd1, 0x11, 0x22, 0x33, 0x44}, 0x12a2L, 7),
            new PackedNumberTestData(new byte[]{(byte)0xfd, (byte)0xa2, 0x12, 0x45, 0x11, (byte)0xd1, 0x11, 0x22, 0x33, 0x44}, 0x4512a2L, 6),
            new PackedNumberTestData(new byte[]{(byte)0xfe, (byte)0xa2, 0x12, 0x45, 0x11, (byte)0x00, 0x00, 0x00, 0x00, 0x44}, 0x114512a2L, 1),
        };

        for (int i = 0; i < testData.length; i++) {
            PackedNumberTestData currentTest = testData[i];
            BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(currentTest.inputBytes));

            long value = reader.readPackedInteger();

            assertEquals(String.format("Unexpected packed int in case %d", i), currentTest.expectedValue, value);
            assertEquals(String.format("Unexpected bytes left in case %d", i), currentTest.expectedAvailableBytes, reader.available());
        }
    }

    @Test(expected = IOException.class)
    public void readPackedInteger_shouldThrowExceptionOnOverflow() throws IOException {
        byte[] data = {(byte)0xfe, (byte)0xa2, 0x12, 0x45, 0x11, (byte)0xd1, 0x11, 0x22, 0x33, 0x44};
        BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(data));

        reader.readPackedInteger();
    }

    @Test
    public void readString_shouldProperlyDecodeStrings() {
        byte[] data = {0x66, 0x6f, 0x6f, 0x2d, 0x62, 0x61, 0x72, 0x12, 0x61};
        BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(data));

        String value = reader.readString(7);

        assertEquals("foo-bar", value);
        assertEquals(2, reader.available());
    }

    @Test
    public void readLengthEncodedString_shouldProperlyDecodeStrings() throws IOException {
        byte[] data = {0x07, 0x66, 0x6f, 0x6f, 0x2d, 0x62, 0x61, 0x72, 0x12, 0x61};
        BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(data));

        String value = reader.readLengthEncodedString();

        assertEquals("foo-bar", value);
        assertEquals(2, reader.available());
    }

    @Test
    public void readZeroTerminatedStringWithHint_shouldProperlyDecodeStrings() throws IOException {
        byte[] data = {0x66, 0x6f, 0x6f, 0x2d, 0x62, 0x61, 0x72, 0x00, 0x12};
        BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(data));

        String value = reader.readZeroTerminatedStringWithHint(7);

        assertEquals("foo-bar", value);
        assertEquals(1, reader.available());
    }

    @Test
    public void readZeroTerminatedString_shouldProperlyDecodeStrings() throws IOException {
        byte[] data = {0x66, 0x6f, 0x6f, 0x2d, 0x62, 0x61, 0x72, 0x00, 0x12};
        BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(data));

        String value = reader.readZeroTerminatedString();

        assertEquals("foo-bar", value);
        assertEquals(1, reader.available());
    }

    @Test(expected = IOException.class)
    public void readZeroTerminatedString_shouldThrowIOExceptionWithNoZeroByte() throws IOException {
        byte[] data = {0x66, 0x6f, 0x6f, 0x2d, 0x62, 0x61, 0x72, 0x12};
        BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(data));

        reader.readZeroTerminatedString();
    }

    @Test(expected = IOException.class)
    public void readZeroTerminatedStringWithHint_shouldThrowExceptionIfStringIsNotZeroTerminated() throws IOException {
        byte[] data = {0x66, 0x6f, 0x6f, 0x2d, 0x62, 0x61, 0x72, 0x12};
        BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(data));

        reader.readZeroTerminatedStringWithHint(7);
    }

    @Test
    public void readBitSet_shouldProperlyDecodeLittleEndianBitSets() {
        BitSetTestData[] testData = {
            // 0101 1101 0001 0001 -> read 1 byte
            // 0101 1101 -> take 8 bits
            // XXXX 1101 -> 0, 2, 3 bits set
            new BitSetTestData(
                new byte[]{0x5d, 0x11}, 4, new int[]{0, 2, 3}, 1),
            // 0001 0001 0010 0010 0011 0011 -> read 2 bytes
            // 0001 0001 0010 0010 -> take 11 bits
            // XXXX X001 0010 0010 -> 1, 5, 8 bits set
            new BitSetTestData(
                new byte[]{0x11, 0x22, 0x33}, 11, new int[]{1, 5, 8}, 1),
            // 0000 1111 1010 1010 0011 0011 -> read 3 bytes
            // 0000 1111 1010 1010 0011 0011 -> take 20 bits
            // XXXX 1111 1010 1010 0011 0011 -> 0, 1, 4, 5, 9, 11, 13, 15, 16, 17, 18, 19 bits set
            new BitSetTestData(
                new byte[]{0x0f, (byte)0xaa, 0x33}, 20, new int[]{0, 1, 4, 5, 9, 11, 13, 15, 16, 17, 18, 19}, 0),
        };

        for (int i = 0; i < testData.length; i++) {
            BitSetTestData currentTest = testData[i];
            BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(currentTest.inputBytes));

            BitSet value = reader.readBitSet(currentTest.bitLength, false);
            BitSet expectedValue = IntStream.of(currentTest.expectedBitsSet).collect(BitSet::new, BitSet::set, BitSet::or);

            assertEquals(String.format("Unexpected bits set in case %d", i), expectedValue, value);
            assertEquals(String.format("Unexpected bytes left in case %d", i), currentTest.expectedAvailableBytes, reader.available());
        }
    }

    @Test
    public void readBitSet_shouldProduceResultsEqualToByteInputStreamForLittleEndian() throws IOException {
        byte[][] inputs = {
            new byte[]{0x5d, 0x11},
            new byte[]{0x11, 0x22, 0x33},
            new byte[]{0x0f, (byte)0xaa, 0x33}
        };

        int[] bitLengths = {4, 11, 20};

        for (int i = 0; i < inputs.length; i++) {
            BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(inputs[i]));
            ByteArrayInputStream stream = new ByteArrayInputStream(inputs[i]);

            BitSet readerBitSet = reader.readBitSet(bitLengths[i], false);
            BitSet streamBitSet = stream.readBitSet(bitLengths[i], false);

            // We assume that ByteArrayInputStream readBitSet is correct
            assertEquals(streamBitSet, readerBitSet);
        }
    }

    @Test
    public void readBitSet_shouldProperlyDecodeBigEndianBitSets() {
        BitSetTestData[] testData = {
            // 0101 1101 0001 0001 -> read 1 byte
            // 0101 1101 -> take 8 bits
            // XXXX 1101 -> 0, 2, 3 bits set
            new BitSetTestData(
                new byte[]{0x5d, 0x11}, 4, new int[]{0, 2, 3}, 1),
            // 0001 0001 0010 0010 0011 0011 -> read 2 bytes
            // 0010 0010 0001 0001 -> take 11 bits
            // XXXX X010 0001 0001 -> 0, 4, 9 bits set
            new BitSetTestData(
                new byte[]{0x11, 0x22, 0x33}, 11, new int[]{0, 4, 9}, 1),
            // 0000 1111 1010 1010 0011 0011 -> read 3 bytes
            // 0011 0011 1010 1010 0000 1111 -> take 20 bits
            // XXXX 0011 1010 1010 0000 1111 -> 0, 1, 2, 3, 9, 11, 13, 15, 16, 17 bits set
            new BitSetTestData(
                new byte[]{0x0f, (byte)0xaa, 0x33}, 20, new int[]{0, 1, 2, 3, 9, 11, 13, 15, 16, 17}, 0),
        };

        for (int i = 0; i < testData.length; i++) {
            BitSetTestData currentTest = testData[i];
            BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(currentTest.inputBytes));

            BitSet value = reader.readBitSet(currentTest.bitLength, true);
            BitSet expectedValue = IntStream.of(currentTest.expectedBitsSet).collect(BitSet::new, BitSet::set, BitSet::or);

            assertEquals(String.format("Unexpected bits set in case %d", i), expectedValue, value);
            assertEquals(String.format("Unexpected bytes left in case %d", i), currentTest.expectedAvailableBytes, reader.available());
        }
    }

    @Test
    public void readBitSet_shouldProduceResultsEqualToByteInputStreamForBigEndian() throws IOException {
        byte[][] inputs = {
            new byte[]{0x5d, 0x11},
            new byte[]{0x11, 0x22, 0x33},
            new byte[]{0x0f, (byte)0xaa, 0x33}
        };

        int[] bitLengths = {4, 11, 20};

        for (int i = 0; i < inputs.length; i++) {
            BinaryLogEventDataReader reader = new BinaryLogEventDataReader(ByteBuffer.wrap(inputs[i]));
            ByteArrayInputStream stream = new ByteArrayInputStream(inputs[i]);

            BitSet readerBitSet = reader.readBitSet(bitLengths[i], true);
            BitSet streamBitSet = stream.readBitSet(bitLengths[i], true);

            // We assume that ByteArrayInputStream readBitSet is correct
            assertEquals(streamBitSet, readerBitSet);
        }
    }

    @Test
    public void enterBlock_shouldOverwritePreviousValue() {
        byte[] data = {0x66, 0x6f, 0x6f, 0x2d, 0x62, 0x61, 0x72, 0x12};
        BinaryLogEventDataReader reader = new BinaryLogEventDataReader(data);

        assertEquals(8, reader.available());
        assertEquals(0x66, reader.peekUnsignedByte());

        reader.enterBlock(4);

        assertEquals(4, reader.available());
        assertEquals(0x66, reader.peekUnsignedByte());

        reader.enterBlock(6);

        assertEquals(6, reader.available());
        assertEquals(0x66, reader.peekUnsignedByte());
    }


    @Test
    public void skipToTheEndOfTheBlock_shouldSkipBytesWithPreviouslyEnterBlock() {
        byte[] data = {0x66, 0x6f, 0x6f, 0x2d, 0x62, 0x61, 0x72, 0x12};
        BinaryLogEventDataReader reader = new BinaryLogEventDataReader(data);

        reader.enterBlock(5);
        reader.skipToTheEndOfTheBlock();

        assertEquals(3, reader.available());
        assertEquals(0x61, reader.peekUnsignedByte());
    }

    @Test
    public void skipToTheEndOfTheBlock_shouldDoNothingWithoutPriorEnterBlock() {
        byte[] data = {0x66, 0x6f, 0x6f, 0x2d, 0x62, 0x61, 0x72, 0x12};
        BinaryLogEventDataReader reader = new BinaryLogEventDataReader(data);

        reader.skipToTheEndOfTheBlock();

        assertEquals(8, reader.available());
        assertEquals(0x66, reader.peekUnsignedByte());
    }

    private static class BitSetTestData {
        public byte[] inputBytes;
        public int bitLength;
        public int[] expectedBitsSet;
        public int expectedAvailableBytes;

        public BitSetTestData(byte[] inputBytes, int bitLength, int[] expectedBitsSet, int expectedAvailableBytes) {
            this.inputBytes = inputBytes;
            this.bitLength = bitLength;
            this.expectedBitsSet = expectedBitsSet;
            this.expectedAvailableBytes = expectedAvailableBytes;
        }
    }

    private static class PackedNumberTestData {
        public byte[] inputBytes;
        public long expectedValue;
        public int expectedAvailableBytes;

        public PackedNumberTestData(byte[] inputBytes, long expectedValue, int expectedAvailableBytes) {
            this.inputBytes = inputBytes;
            this.expectedValue = expectedValue;
            this.expectedAvailableBytes = expectedAvailableBytes;
        }
    }
}
