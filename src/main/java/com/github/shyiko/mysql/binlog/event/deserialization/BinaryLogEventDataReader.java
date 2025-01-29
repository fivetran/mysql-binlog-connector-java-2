package com.github.shyiko.mysql.binlog.event.deserialization;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;

/**
 * The class is used to read raw binary log data in an effective way. It is used by deserializers to build actual events.
 * The class relies on {@link ByteBuffer}'s read methods being faster than those of the InputStream.
 */
public class BinaryLogEventDataReader {
    /**
     * The buffer uses {@link ByteOrder#LITTLE_ENDIAN} byte order by default because MySQL mostly uses LE for encoding.
     */
    private final ByteBuffer buffer;

    private int oldLimit = -1;

    public BinaryLogEventDataReader(ByteBuffer buffer) {
        this.buffer = buffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    public BinaryLogEventDataReader(byte[] bytes) {
        this.buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * Reads {@code length} bytes from the current position in the buffer, interpreting {@code 8 - skipLength} bytes
     * as little-endian long returning the value.
     * This method acts as an optimization for skipping reserved bytes which is quite common in MySQL protocol.
     */
    public long readLongWithSkip(int skipLength) {
        // Example for skipLength = 2:
        // byte[](0x11 0x22 0x33 0x44 0x55 0x66 0x77 0x88) -> long(0x8877665544332211)
        // 0x8877665544332211 & 0x0000ffffffffffff -> 0x665544332211
        long value = buffer.getLong();
        long mask = (1L << ((8 - skipLength) * 8)) - 1;
        return value & mask;
    }

    /**
     * Reads {@code length} bytes from the current position in the buffer and interprets it as little-endian long
     * returning the value.
     */
    public long readLong(int length) {
        // Specific getXXX methods are usually faster due to internal use of Unsafe.
        switch (length) {
            case 8:
                return buffer.getLong();
            case 4:
                return buffer.getInt() & 0xFFFFFFFFL;
            case 2:
                return buffer.getShort() & 0xFFFFL;
            case 1:
                return buffer.get() & 0xFFL;
        }

        long result = 0;
        for (int i = 0; i < length; i++) {
            result |= (buffer.get() & 0xFFL) << (i << 3);
        }
        return result;
    }

    /**
     * Reads {@code length} bytes from the current position in the buffer and interprets it as big-endian long
     * returning the value.
     */
    public long readLongBE(int length) {
        switch (length) {
            case 8:
                return Long.reverseBytes(buffer.getLong());
            case 4:
                return Integer.reverseBytes(buffer.getInt()) & 0xFFFFFFFFL;
            case 2:
                return Short.reverseBytes(buffer.getShort()) & 0xFFFFL;
            case 1:
                return buffer.get() & 0xFFL;
        }

        long result = 0;
        for (int i = 0; i < length; i++) {
            result <<= 8;
            result |= buffer.get() & 0xFFL;
        }
        return result;
    }

    /**
     * Reads {@code length} bytes from the current position in the buffer and interprets it as little-endian int
     * returning the value.
     */
    public int readInteger(int length) {
        // Specific getXXX methods are usually faster due to internal use of Unsafe.
        switch (length) {
            case 4:
                return buffer.getInt();
            case 2:
                return buffer.getShort() & 0xFFFF;
            case 1:
                return buffer.get() & 0xFF;
        }

        int result = 0;
        for (int i = 0; i < length; i++) {
            result |= (buffer.get() & 0xFF) << (i << 3);
        }
        return result;
    }

    /**
     * Reads {@code length} bytes from the current position in the buffer and interprets it as big-endian int
     * returning the value.
     */
    public int readIntegerBE(int length) {
        switch (length) {
            case 4:
                return Integer.reverseBytes(buffer.getInt());
            case 2:
                return Short.reverseBytes(buffer.getShort()) & 0xFFFF;
            case 1:
                return buffer.get() & 0xFF;
        }

        int result = 0;
        for (int i = 0; i < length; i++) {
            result <<= 8;
            result |= buffer.get() & 0xFF;
        }
        return result;
    }

    public int readUnsignedByte() {
        return buffer.get() & 0xFF;
    }

    /**
     * Returns the value of the byte at the current position in the buffer or -1 if there are no bytes left.
     */
    public int peekUnsignedByte() {
        if (!buffer.hasRemaining()) {
            return -1;
        }
        return buffer.get(buffer.position()) & 0xFF;
    }

    public byte readByte() {
        return buffer.get();
    }

    /**
     * Reads {@code length} bytes from the current position in the buffer returning them as byte array.
     */
    public byte[] readBytes(int length) {
        byte[] bytes = new byte[length];
        buffer.get(bytes, 0, bytes.length);
        return bytes;
    }

    public void skip(int length) {
        buffer.position(buffer.position() + length);
    }

    /**
     * Reads packed long value from the current position in the buffer.
     * <p>
     * Format (first-byte-based):<br>
     * 0-250 - The first byte is the number (in the range 0-250). No additional bytes are used.<br>
     * 251 - SQL NULL value<br>
     * 252 - Two more bytes are used. The number is in the range 251-0xffff.<br>
     * 253 - Three more bytes are used. The number is in the range 0xffff-0xffffff.<br>
     * 254 - Eight more bytes are used. The number is in the range 0xffffff-0xffffffffffffffff.
     * @return long or null
     * @throws IOException if unexpected bytes are encountered (IOException is used for compatibility with
     *                     {@link com.github.shyiko.mysql.binlog.io.ByteArrayInputStream}.
     */
    public long readPackedLong() throws IOException {
        int b = (buffer.get() & 0xFF);
        if (b < 251) {
            return b;
        } else if (b == 251) {
            throw new IOException("Unexpected NULL value");
        } else if (b == 252) {
            return (buffer.getShort() & 0xFFFFL);
        } else if (b == 253) {
            return (buffer.get() & 0xFFL) | ((buffer.get() & 0xFFL) << 8) | ((buffer.get() & 0xFFL) << 16);
        } else if (b == 254) {
            return buffer.getLong();
        }
        throw new RuntimeException("Unexpected packed number byte " + b);
    }

    /**
     * Reads packed integer value from the current position in the buffer. Similar to {@link #readPackedLong()} but
     * makes sure that the number read is fit into integer.
     * @throws IOException if unexpected bytes are encountered or in case of integer overflow (IOException is used for
     *                      compatibility with {@link com.github.shyiko.mysql.binlog.io.ByteArrayInputStream}.
     */
    public int readPackedInteger() throws IOException {
        long value = readPackedLong();
        if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
            throw new IOException("Packed integer " + value + " is out of Integer range");
        }
        return (int)value;
    }


    public BitSet readBitSet(int length, boolean bigEndian) {
        // According to MySQL internals the amount of storage required for N columns is INT((N+7)/8) bytes.
        int lengthInBytes = (length + 7) >> 3;
        byte[] bytes = new byte[lengthInBytes];
        buffer.get(bytes);

        // BitSet.valueOf we're using below interprets bytes as little-endian
        if (!bigEndian) {
            bytes = reverse(bytes);
        }

        // Zero out not used bits.
        byte mask = (byte)(0xff >> (bytes.length * 8 - length));
        bytes[bytes.length - 1] = (byte)(bytes[bytes.length - 1] & mask);

        return BitSet.valueOf(bytes);
    }

    public int available() {
        return buffer.remaining();
    }

    /**
     * Reads a string of size {@code length} from the current position in the buffer.
     */
    public String readString(int length) {
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes);
    }

    /**
     * Reads a string prepended by a packed integer length from the current position in the buffer.
     */
    public String readLengthEncodedString() throws IOException {
        return readString(readPackedInteger());
    }

    /**
     * Reads a zero-terminated string from the current position in the buffer.
     */
    public String readZeroTerminatedString() throws IOException {
        int zeroIndex = indexOf((byte)0);
        if (zeroIndex == -1) {
            throw new IOException("End of buffer reached with no zero byte found");
        }

        int length = zeroIndex - buffer.position();
        String value = readString(length);
        buffer.position(buffer.position() + 1);
        return value;
    }

    /**
     * Reads a zero-terminated string of length {@code lengthHint} from the current position in the buffer.
     * This method is usually faster than {@link #readZeroTerminatedString()} because we can avoid searching for a
     * zero byte.
     */
    public String readZeroTerminatedStringWithHint(int lengthHint) throws IOException {
        String value = readString(lengthHint);
        if (buffer.get() != 0) {
            throw new IOException("Zero-terminated string of size " + lengthHint + " has no terminator");
        }
        return value;
    }

    /**
     * Limits the number of bytes that can be read from the current position of the buffer to {@code length}.
     * It is used to parse length-encoded chunks of events. It effectively changes the limit of the buffer preserving
     * the original one.
     */
    public void enterBlock(int length) {
        if (oldLimit == -1) {
            oldLimit = buffer.limit();
        }
        buffer.limit(buffer.position() + length);
    }

    /**
     * Skips all the bytes in the buffer up to the point provided via previous {@link #enterBlock(int)} call.
     * Does nothing if {@link #enterBlock(int)} was not called.
     */
    public void skipToTheEndOfTheBlock() {
        if (oldLimit != -1) {
            buffer.position(buffer.limit());
            buffer.limit(oldLimit);
            oldLimit = -1;
        }
    }

    private int indexOf(byte b) {
        for (int i = buffer.position(); i < buffer.limit(); i++) {
            if (buffer.get(i) == b) {
                return i;
            }
        }
        return -1;
    }

    private static byte[] reverse(byte[] bytes) {
        for (int i = 0, j = bytes.length - 1; i < j; i++, j--) {
            byte tmp = bytes[i];
            bytes[i] = bytes[j];
            bytes[j] = tmp;
        }
        return bytes;
    }

}
