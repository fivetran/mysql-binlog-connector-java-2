package com.github.shyiko.mysql.binlog.io;

import com.github.shyiko.mysql.binlog.event.RawBinaryLogEvent;
import com.github.shyiko.mysql.binlog.network.ServerException;
import com.github.shyiko.mysql.binlog.network.protocol.ErrorPacket;

import java.io.IOException;
import java.util.Arrays;

public class RawEventsReader {
    // https://dev.mysql.com/doc/internals/en/sending-more-than-16mbyte.html
    private static final int MAX_PACKET_LENGTH = 16777215;

    private final ByteArrayInputStream inputStream;

    public RawEventsReader(ByteArrayInputStream inputStream) {
        this.inputStream = inputStream;
    }

    /**
     * Reads next {@link RawBinaryLogEvent} from the stream returning the event or {@code null} if the end of the stream
     * is reached.
     * @throws IOException on stream read error.
     */
    public RawBinaryLogEvent nextRawEvent() throws IOException {
        // End of stream reached.
        if (inputStream.peek() == -1) {
            return null;
        }

        int packetLength = readPacketLength();
        if (packetLength == -1) {
            return null;
        }

        byte[] packetBytes = new byte[packetLength];

        // We always skip 1 byte from the first packet due to status byte that is only present in the first packet.
        int chunkLength = packetLength - 1;
        int currentPosition = 0;
        while (packetLength == MAX_PACKET_LENGTH) {
            inputStream.fill(packetBytes, currentPosition, chunkLength);
            currentPosition += chunkLength;

            packetLength = inputStream.readInteger(3);
            inputStream.skip(1);

            chunkLength = packetLength;
            packetBytes = Arrays.copyOf(packetBytes, packetBytes.length + chunkLength);
        }

        inputStream.fill(packetBytes, currentPosition, chunkLength);

        return new RawBinaryLogEvent(packetBytes);
    }

    private int readPacketLength() throws IOException {
        // Read packet size (3 bytes), sequence number (1 byte, ignored) and marker (1 byte).
        // We read all data at once to reduce the number of reads from Socket stream.
        byte[] packetHeaderBytes = new byte[5];
        inputStream.fill(packetHeaderBytes, 0, packetHeaderBytes.length);

        int packetLength = 0;
        for (int i = 0; i < 3; ++i) {
            packetLength |= (packetHeaderBytes[i] & 0xFF) << (i << 3);
        }

        int marker = packetHeaderBytes[4] & 0xFF;
        if (marker == 0xFF) {
            ErrorPacket errorPacket = new ErrorPacket(inputStream.read(packetLength - 1));
            throw new ServerException(errorPacket.getErrorMessage(), errorPacket.getErrorCode(),
                errorPacket.getSqlState());
        }

        if (marker == 0xFE) {
            return -1;
        }

        return packetLength;
    }
}