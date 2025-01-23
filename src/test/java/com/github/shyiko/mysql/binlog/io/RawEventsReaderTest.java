package com.github.shyiko.mysql.binlog.io;

import com.github.shyiko.mysql.binlog.event.RawBinaryLogEvent;
import com.github.shyiko.mysql.binlog.event.deserialization.BinaryLogEventDataReader;
import com.github.shyiko.mysql.binlog.network.ServerException;

import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;

public class RawEventsReaderTest {

    @Test
    public void nextRawEvent_shouldReturnNull_onNoMoreData() throws IOException {
        byte[] bytes = {};
        RawEventsReader rawEventsReader = new RawEventsReader(new ByteArrayInputStream(bytes));

        RawBinaryLogEvent rawEvent = rawEventsReader.nextRawEvent();

        assertNull(rawEvent);
    }

    @Test
    public void nextRawEvent_shouldReturnEof_onEofPacketReceived() throws IOException {
        byte[] bytes = {0x00, 0x00, 0x00, 0x00, (byte)0xfe};
        RawEventsReader rawEventsReader = new RawEventsReader(new ByteArrayInputStream(bytes));

        RawBinaryLogEvent rawEvent = rawEventsReader.nextRawEvent();

        assertTrue(rawEvent.isServerEof());
    }

    @Test(expected = IOException.class)
    public void nextRawEvent_shouldThrowIOException_onInputStreamException() throws IOException {
        ByteArrayInputStream mockStream = Mockito.mock(ByteArrayInputStream.class);
        when(mockStream.peek()).thenReturn(0x01);
        doThrow(IOException.class).when(mockStream).fill(any(), anyInt(), anyInt());
        RawEventsReader rawEventsReader = new RawEventsReader(mockStream);

        rawEventsReader.nextRawEvent();
    }

    @Test(expected = ServerException.class)
    public void nextRawEvent_shouldThrowServerException_onServerError() throws IOException {
        byte[] bytes = {
            0x15, 0x00, 0x00, 0x00, (byte)0xff, 0x01, 0x01, 0x23, 0x48, 0x59, 0x30, 0x31, 0x39,
            0x6b, 0x65, 0x72, 0x6e, 0x65, 0x6c, 0x5f, 0x70, 0x61, 0x6e, 0x69, 0x63
        };
        RawEventsReader rawEventsReader = new RawEventsReader(new ByteArrayInputStream(bytes));

        RawBinaryLogEvent rawEvent = rawEventsReader.nextRawEvent();
    }

    @Test
    public void nextRawEvent_shouldReturnRawEvent_onOnePacketEvent() throws IOException {
        byte[] bytes = {
            0x09, 0x00, 0x00, 0x00, 0x00,
            0x0f, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
        };
        RawEventsReader rawEventsReader = new RawEventsReader(new ByteArrayInputStream(bytes));

        RawBinaryLogEvent rawEvent = rawEventsReader.nextRawEvent();

        assertFalse(rawEvent.isServerEof());
        BinaryLogEventDataReader dataReader = rawEvent.getEventDataReader();
        byte[] eventBytes = dataReader.readBytes(dataReader.available());

        byte[] expectedEventBytes = {0x0f, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
        assertArrayEquals(expectedEventBytes, eventBytes);
    }

    @Test
    public void nextRawEvent_shouldReturnRawEvent_onTwoPacketEvent() throws IOException {
        byte[] eventBytes = generateMultiPacketData();
        byte[] packetBytes = eventBytesToPacketBytes(eventBytes);

        RawEventsReader rawEventsReader = new RawEventsReader(new ByteArrayInputStream(packetBytes));

        RawBinaryLogEvent rawEvent = rawEventsReader.nextRawEvent();

        assertFalse(rawEvent.isServerEof());
        BinaryLogEventDataReader dataReader = rawEvent.getEventDataReader();
        byte[] actualEventBytes = dataReader.readBytes(dataReader.available());

        assertArrayEquals(eventBytes, actualEventBytes);
    }

    private static byte[] generateMultiPacketData() {
        final int firstPacketSize = RawEventsReader.MAX_PACKET_LENGTH - 1;
        final int secondPacketSize = 32;

        byte[] eventBytes = new byte[firstPacketSize + secondPacketSize];
        Arrays.fill(eventBytes, 0, firstPacketSize, (byte)0x11);
        Arrays.fill(eventBytes, firstPacketSize, eventBytes.length, (byte)0x22);

        return eventBytes;
    }

    private static byte[] eventBytesToPacketBytes(byte[] eventBytes) {
        int numPackets = eventBytes.length / RawEventsReader.MAX_PACKET_LENGTH + 1;
        int packetHeadersLength = (numPackets - 1) * 4 + 5;

        byte[] packetBytes = new byte[eventBytes.length + packetHeadersLength];
        int eventBytesLeft = eventBytes.length;
        int packetBytesOffset = 0;
        int packetNumber = 0;
        while (eventBytesLeft > 0) {
            int packetLength = Math.min(eventBytesLeft, RawEventsReader.MAX_PACKET_LENGTH);
            packetBytes[packetBytesOffset] = (byte) (packetLength & 0xFF);
            packetBytes[packetBytesOffset+1] = (byte) ((packetLength >>> 8) & 0xFF);
            packetBytes[packetBytesOffset+2] = (byte) ((packetLength >>> 16) & 0xFF);

            packetBytesOffset += 3;
            packetBytes[packetBytesOffset++] = (byte)packetNumber;
            if (packetNumber == 0) {
                packetBytes[packetBytesOffset++] = 0x00;
                packetLength--;
            }
            System.arraycopy(
                eventBytes,
                eventBytes.length - eventBytesLeft,
                packetBytes,
                packetBytesOffset,
                packetLength);

            packetBytesOffset += packetLength;
            eventBytesLeft -= packetLength;
            packetNumber++;
        }

        return packetBytes;
    }
}
