/*
 * Copyright 2013 Patrick Prasse
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.MySqlGtid;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.UUID;

/**
 * @author <a href="mailto:pprasse@actindo.de">Patrick Prasse</a>
 */
public class GtidEventDataDeserializer implements EventDataDeserializer<GtidEventData> {
    public static final int LOGICAL_TIMESTAMP_TYPECODE_LENGTH = 1;
    // Type code used before the logical timestamps.
    public static final int LOGICAL_TIMESTAMP_TYPECODE = 2;
    public static final int LOGICAL_TIMESTAMP_LENGTH = 8;
    // Length of immediate and original commit timestamps
    public static final int IMMEDIATE_COMMIT_TIMESTAMP_LENGTH = 7;
    public static final int ORIGINAL_COMMIT_TIMESTAMP_LENGTH = 7;
    // Use 7 bytes out of which 1 bit is used as a flag.
    public static final int ENCODED_COMMIT_TIMESTAMP_LENGTH = 55;
    public static final int TRANSACTION_LENGTH_MIN_LENGTH = 1;
    // Length of immediate and original server versions
    public static final int IMMEDIATE_SERVER_VERSION_LENGTH = 4;
    public static final int ORIGINAL_SERVER_VERSION_LENGTH = 4;
    // Use 4 bytes out of which 1 bit is used as a flag.
    public static final int ENCODED_SERVER_VERSION_LENGTH = 31;
    public static final int UNDEFINED_SERVER_VERSION = 999999;

    @Override
    public GtidEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
        byte flags = (byte) inputStream.readInteger(1);
        long sourceIdMostSignificantBits = readLongBigEndian(inputStream);
        long sourceIdLeastSignificantBits = readLongBigEndian(inputStream);
        long transactionId = inputStream.readLong(8);

        final MySqlGtid gtid = new MySqlGtid(
                new UUID(sourceIdMostSignificantBits, sourceIdLeastSignificantBits),
                transactionId
            );

        // MTR logical clock
        long lastCommitted = 0;
        long sequenceNumber = 0;
        // ImmediateCommitTimestamp/OriginalCommitTimestamp are introduced in MySQL-8.0.1, see:
        // https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-1.html
        long immediateCommitTimestamp = 0;
        long originalCommitTimestamp = 0;
        // Total transaction length (including this GTIDEvent), introduced in MySQL-8.0.2, see:
        // https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-2.html
        long transactionLength = 0;
        // ImmediateServerVersion/OriginalServerVersion are introduced in MySQL-8.0.14, see
        // https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-14.html
        int immediateServerVersion = 0;
        int originalServerVersion = 0;

        // Logical timestamps - since MySQL 5.7.6
        if (inputStream.peek() == LOGICAL_TIMESTAMP_TYPECODE) {
            inputStream.skip(LOGICAL_TIMESTAMP_TYPECODE_LENGTH);
            lastCommitted = inputStream.readLong(LOGICAL_TIMESTAMP_LENGTH);
            sequenceNumber = inputStream.readLong(LOGICAL_TIMESTAMP_LENGTH);
            // Immediate and original commit timestamps are introduced in MySQL-8.0.1
            if (inputStream.available() >= IMMEDIATE_COMMIT_TIMESTAMP_LENGTH) {
                immediateCommitTimestamp = inputStream.readLong(IMMEDIATE_COMMIT_TIMESTAMP_LENGTH);
                // Check the MSB to determine how to populate the original commit timestamp
                if ((immediateCommitTimestamp & (1L << ENCODED_COMMIT_TIMESTAMP_LENGTH)) != 0) {
                    immediateCommitTimestamp &= ~(1L << ENCODED_COMMIT_TIMESTAMP_LENGTH);
                    originalCommitTimestamp = inputStream.readLong(ORIGINAL_COMMIT_TIMESTAMP_LENGTH);
                } else {
                    // Transaction originated in the previous server eg. writer if direct connect
                    originalCommitTimestamp = immediateCommitTimestamp;
                }
                // Total transaction length (including this GTIDEvent), introduced in MySQL-8.0.2
                if (inputStream.available() >= TRANSACTION_LENGTH_MIN_LENGTH) {
                    transactionLength = inputStream.readPackedLong();
                }
                immediateServerVersion = UNDEFINED_SERVER_VERSION;
                originalServerVersion = UNDEFINED_SERVER_VERSION;
                // Immediate and original server versions are introduced in MySQL-8.0.14
                if (inputStream.available() >= IMMEDIATE_SERVER_VERSION_LENGTH) {
                    immediateServerVersion = inputStream.readInteger(IMMEDIATE_SERVER_VERSION_LENGTH);
                    // Check the MSB to determine how to populate original server version
                    if ((immediateServerVersion & (1L << ENCODED_SERVER_VERSION_LENGTH)) != 0) {
                        immediateServerVersion &= ~(1L << ENCODED_SERVER_VERSION_LENGTH);
                        originalServerVersion = inputStream.readInteger(ORIGINAL_SERVER_VERSION_LENGTH);
                    } else {
                        originalServerVersion = immediateServerVersion;
                    }
                }
            }
        }

        return new GtidEventData(gtid, flags, lastCommitted, sequenceNumber, immediateCommitTimestamp, originalCommitTimestamp, transactionLength, immediateServerVersion, originalServerVersion);
    }

    @Override
    public GtidEventData deserialize(BinaryLogEventDataReader eventDataReader) throws IOException {
        byte flags = eventDataReader.readByte();
        long sourceIdMostSignificantBits = eventDataReader.readLongBE(8);
        long sourceIdLeastSignificantBits = eventDataReader.readLongBE(8);
        long transactionId = eventDataReader.readLong(8);

        final MySqlGtid gtid = new MySqlGtid(
            new UUID(sourceIdMostSignificantBits, sourceIdLeastSignificantBits),
            transactionId
        );

        // MTR logical clock
        long lastCommitted = 0;
        long sequenceNumber = 0;
        // ImmediateCommitTimestamp/OriginalCommitTimestamp are introduced in MySQL-8.0.1, see:
        // https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-1.html
        long immediateCommitTimestamp = 0;
        long originalCommitTimestamp = 0;
        // Total transaction length (including this GTIDEvent), introduced in MySQL-8.0.2, see:
        // https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-2.html
        long transactionLength = 0;
        // ImmediateServerVersion/OriginalServerVersion are introduced in MySQL-8.0.14, see
        // https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-14.html
        int immediateServerVersion = 0;
        int originalServerVersion = 0;

        // Logical timestamps - since MySQL 5.7.6
        if (eventDataReader.peekUnsignedByte() == LOGICAL_TIMESTAMP_TYPECODE) {
            eventDataReader.skip(LOGICAL_TIMESTAMP_TYPECODE_LENGTH);
            lastCommitted = eventDataReader.readLong(LOGICAL_TIMESTAMP_LENGTH);
            sequenceNumber = eventDataReader.readLong(LOGICAL_TIMESTAMP_LENGTH);
            // Immediate and original commit timestamps are introduced in MySQL-8.0.1
            if (eventDataReader.available() >= IMMEDIATE_COMMIT_TIMESTAMP_LENGTH) {
                immediateCommitTimestamp = eventDataReader.readLong(IMMEDIATE_COMMIT_TIMESTAMP_LENGTH);
                // Check the MSB to determine how to populate the original commit timestamp
                if ((immediateCommitTimestamp & (1L << ENCODED_COMMIT_TIMESTAMP_LENGTH)) != 0) {
                    immediateCommitTimestamp &= ~(1L << ENCODED_COMMIT_TIMESTAMP_LENGTH);
                    originalCommitTimestamp = eventDataReader.readLong(ORIGINAL_COMMIT_TIMESTAMP_LENGTH);
                } else {
                    // Transaction originated in the previous server eg. writer if direct connect
                    originalCommitTimestamp = immediateCommitTimestamp;
                }
                // Total transaction length (including this GTIDEvent), introduced in MySQL-8.0.2
                if (eventDataReader.available() >= TRANSACTION_LENGTH_MIN_LENGTH) {
                    transactionLength = eventDataReader.readPackedLong();
                }
                immediateServerVersion = UNDEFINED_SERVER_VERSION;
                originalServerVersion = UNDEFINED_SERVER_VERSION;
                // Immediate and original server versions are introduced in MySQL-8.0.14
                if (eventDataReader.available() >= IMMEDIATE_SERVER_VERSION_LENGTH) {
                    immediateServerVersion = eventDataReader.readInteger(IMMEDIATE_SERVER_VERSION_LENGTH);
                    // Check the MSB to determine how to populate original server version
                    if ((immediateServerVersion & (1L << ENCODED_SERVER_VERSION_LENGTH)) != 0) {
                        immediateServerVersion &= ~(1L << ENCODED_SERVER_VERSION_LENGTH);
                        originalServerVersion = eventDataReader.readInteger(ORIGINAL_SERVER_VERSION_LENGTH);
                    } else {
                        originalServerVersion = immediateServerVersion;
                    }
                }
            }
        }

        return new GtidEventData(gtid, flags, lastCommitted, sequenceNumber, immediateCommitTimestamp, originalCommitTimestamp, transactionLength, immediateServerVersion, originalServerVersion);
    }

    private static long readLongBigEndian(ByteArrayInputStream input) throws IOException {
        long result = 0;
        for (int i = 0; i < 8; ++i) {
            result = ((result << 8) | (input.read() & 0xff));
        }
        return result;
    }

}
