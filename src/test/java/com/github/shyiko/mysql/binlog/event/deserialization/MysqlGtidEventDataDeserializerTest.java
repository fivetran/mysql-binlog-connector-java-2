package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.testng.Assert.*;

public class MysqlGtidEventDataDeserializerTest {

    public static final byte[] MYSQL4_BYTES = {
        0x03, //flags
        0x24, (byte) 0xbc, 0x78, 0x50, 0x2c, 0x16, 0x11, (byte) 0xe6, // sourceId mostSignificantBits big endian
        (byte) 0xa0, 0x73, 0x02, 0x42, (byte) 0xac, 0x11, 0x00, 0x02, // sourceId leastSignificantBits big endian
        (byte) 0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 // sequence little endian
    };
    public static final byte[] MYSQL801_BYTES = {
        0x01, // flags
        (byte) 0xaa, (byte) 0xe5, 0x7b, 0x2f, (byte) 0x8e, 0x44, 0x11, (byte) 0xee, // sourceId mostSignificantBits big endian
        (byte) 0xa3, (byte) 0xd6, (byte) 0xa0, 0x36, (byte) 0xbc, (byte) 0xda, 0x1a, 0x41, // sourceId leastSignificantBits big endian
        0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // sequence little endian
        0x02, // MTR
        0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // last committed
        0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // sequence number
        (byte) 0x97, (byte) 0xef, 0x0c, 0x25, 0x3f, 0x0b, 0x06, // commit timestamp
    };
    public static final byte[] MYSQL802_BYTES = {
        0x00, // flags
        (byte) 0x99, 0x4a, (byte) 0xb8, 0x59, (byte) 0x8e, (byte) 0xa8, 0x11, (byte) 0xee, // sourceId mostSignificantBits big endian
        (byte) 0xa5, 0x68, (byte) 0xa0, 0x36, (byte) 0xbc, (byte) 0xda, 0x1a, 0x41, // sourceId leastSignificantBits big endian
        0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // sequence little endian
        0x02, // MTR
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // last committed
        0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // sequence number
        0x40, 0x55, 0x04, (byte) 0xc4, 0x48, 0x0b, 0x06, // commit timestamp
        (byte) 0xfc, 0x34, 0x01, // transaction length
    };
    public static final byte[] MYSQL810_BYTES = {
        0x00, // flags
        (byte) 0xbd, (byte) 0x97, (byte) 0x94, (byte) 0xe0, 0x1d, 0x65, 0x11, (byte) 0xed, // sourceId mostSignificantBits big endian
        (byte) 0xa7, (byte) 0xe7, 0x0a, (byte) 0xdb, 0x30, 0x5b, 0x3a, 0x12, // sourceId leastSignificantBits big endian
        0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // sequence little endian
        0x02, // MTR
        0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // last committed
        0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // sequence number
        0x66, 0x29, (byte) 0xaa, 0x69, 0x55, 0x09, 0x06, // commit timestamp
        (byte) 0xfc, 0x3b, 0x01, // transaction length
        (byte) 0xe4, 0x38, 0x01, 0x00, // immediate server version
    };
    private GtidEventDataDeserializer deserializer = new GtidEventDataDeserializer();

    @Test
    public void testDeserialize() throws IOException {
        GtidEventData data = deserializer.deserialize(new ByteArrayInputStream(MYSQL4_BYTES));
        assertEquals(data.getFlags(), 0x03);
        assertEquals(data.getMySqlGtid().toString(), "24bc7850-2c16-11e6-a073-0242ac110002:11");
        assertEquals(data.getLastCommitted(), 0);
        assertEquals(data.getSequenceNumber(), 0);
        assertEquals(data.toString(), "GtidEventData{flags=3, gtid='24bc7850-2c16-11e6-a073-0242ac110002:11', last_committed='0', sequence_number='0'}");
    }

    @Test
    public void testDeserializeStream() throws IOException {
        GtidEventData data = deserializer.deserialize(new BinaryLogEventDataReader(MYSQL4_BYTES));

        assertEquals(data.getFlags(), 0x03);
        assertEquals(data.getMySqlGtid().toString(), "24bc7850-2c16-11e6-a073-0242ac110002:11");
        assertEquals(data.getLastCommitted(), 0);
        assertEquals(data.getSequenceNumber(), 0);
        assertEquals(data.toString(), "GtidEventData{flags=3, gtid='24bc7850-2c16-11e6-a073-0242ac110002:11', last_committed='0', sequence_number='0'}");
    }

    @Test
    public void testDeserializeMySQL801() throws IOException {
        GtidEventData data = deserializer.deserialize(new ByteArrayInputStream(MYSQL801_BYTES));
        assertEquals(data.getFlags(), 0x01);
        assertEquals(data.getMySqlGtid().toString(), "aae57b2f-8e44-11ee-a3d6-a036bcda1a41:4");
        assertEquals(data.getLastCommitted(), 3);
        assertEquals(data.getSequenceNumber(), 4);
        assertEquals(data.getImmediateCommitTimestamp(), 1701215692713879L);
        assertEquals(data.getOriginalCommitTimestamp(), 1701215692713879L);
        assertEquals(data.getTransactionLength(), 0);
        assertEquals(data.getImmediateServerVersion(), 999999);
        assertEquals(data.getOriginalServerVersion(), 999999);
        assertEquals(data.toString(), "GtidEventData{flags=1, gtid='aae57b2f-8e44-11ee-a3d6-a036bcda1a41:4', last_committed='3', sequence_number='4', immediate_commit_timestamp='1701215692713879', original_commit_timestamp='1701215692713879'}");
    }

    @Test
    public void testDeserializeMySQL801Reader() throws IOException {
        GtidEventData data = deserializer.deserialize(new BinaryLogEventDataReader(MYSQL801_BYTES));
        assertEquals(data.getFlags(), 0x01);
        assertEquals(data.getMySqlGtid().toString(), "aae57b2f-8e44-11ee-a3d6-a036bcda1a41:4");
        assertEquals(data.getLastCommitted(), 3);
        assertEquals(data.getSequenceNumber(), 4);
        assertEquals(data.getImmediateCommitTimestamp(), 1701215692713879L);
        assertEquals(data.getOriginalCommitTimestamp(), 1701215692713879L);
        assertEquals(data.getTransactionLength(), 0);
        assertEquals(data.getImmediateServerVersion(), 999999);
        assertEquals(data.getOriginalServerVersion(), 999999);
        assertEquals(data.toString(), "GtidEventData{flags=1, gtid='aae57b2f-8e44-11ee-a3d6-a036bcda1a41:4', last_committed='3', sequence_number='4', immediate_commit_timestamp='1701215692713879', original_commit_timestamp='1701215692713879'}");
    }

    @Test
    public void testDeserializeMySQL802() throws IOException {
        GtidEventData data = deserializer.deserialize(new ByteArrayInputStream(MYSQL802_BYTES));
        assertEquals(data.getFlags(), 0x00);
        assertEquals(data.getMySqlGtid().toString(), "994ab859-8ea8-11ee-a568-a036bcda1a41:3");
        assertEquals(data.getLastCommitted(), 2);
        assertEquals(data.getSequenceNumber(), 3);
        assertEquals(data.getImmediateCommitTimestamp(), 1701257014433088L);
        assertEquals(data.getOriginalCommitTimestamp(), 1701257014433088L);
        assertEquals(data.getTransactionLength(), 308);
        assertEquals(data.getImmediateServerVersion(), 999999);
        assertEquals(data.getOriginalServerVersion(), 999999);
        assertEquals(data.toString(), "GtidEventData{flags=0, gtid='994ab859-8ea8-11ee-a568-a036bcda1a41:3', last_committed='2', sequence_number='3', immediate_commit_timestamp='1701257014433088', original_commit_timestamp='1701257014433088', transaction_length='308', immediate_server_version='999999', original_server_version='999999'}");
    }

    @Test
    public void testDeserializeMySQL802Reader() throws IOException {
        GtidEventData data = deserializer.deserialize(new BinaryLogEventDataReader(MYSQL802_BYTES));
        assertEquals(data.getFlags(), 0x00);
        assertEquals(data.getMySqlGtid().toString(), "994ab859-8ea8-11ee-a568-a036bcda1a41:3");
        assertEquals(data.getLastCommitted(), 2);
        assertEquals(data.getSequenceNumber(), 3);
        assertEquals(data.getImmediateCommitTimestamp(), 1701257014433088L);
        assertEquals(data.getOriginalCommitTimestamp(), 1701257014433088L);
        assertEquals(data.getTransactionLength(), 308);
        assertEquals(data.getImmediateServerVersion(), 999999);
        assertEquals(data.getOriginalServerVersion(), 999999);
        assertEquals(data.toString(), "GtidEventData{flags=0, gtid='994ab859-8ea8-11ee-a568-a036bcda1a41:3', last_committed='2', sequence_number='3', immediate_commit_timestamp='1701257014433088', original_commit_timestamp='1701257014433088', transaction_length='308', immediate_server_version='999999', original_server_version='999999'}");
    }

    @Test
    public void testDeserializeMySQL810() throws IOException {
        GtidEventData data = deserializer.deserialize(new ByteArrayInputStream(MYSQL810_BYTES));
        assertEquals(data.getFlags(), 0x00);
        assertEquals(data.getMySqlGtid().toString(), "bd9794e0-1d65-11ed-a7e7-0adb305b3a12:9");
        assertEquals(data.getLastCommitted(), 7);
        assertEquals(data.getSequenceNumber(), 8);
        assertEquals(data.getImmediateCommitTimestamp(), 1699112309893478L);
        assertEquals(data.getOriginalCommitTimestamp(), 1699112309893478L);
        assertEquals(data.getTransactionLength(), 315);
        assertEquals(data.getImmediateServerVersion(), 80100);
        assertEquals(data.getOriginalServerVersion(), 80100);
        assertEquals(data.toString(), "GtidEventData{flags=0, gtid='bd9794e0-1d65-11ed-a7e7-0adb305b3a12:9', last_committed='7', sequence_number='8', immediate_commit_timestamp='1699112309893478', original_commit_timestamp='1699112309893478', transaction_length='315', immediate_server_version='80100', original_server_version='80100'}");
    }

    @Test
    public void testDeserializeMySQL810Reader() throws IOException {
        GtidEventData data = deserializer.deserialize(new BinaryLogEventDataReader(MYSQL810_BYTES));
        assertEquals(data.getFlags(), 0x00);
        assertEquals(data.getMySqlGtid().toString(), "bd9794e0-1d65-11ed-a7e7-0adb305b3a12:9");
        assertEquals(data.getLastCommitted(), 7);
        assertEquals(data.getSequenceNumber(), 8);
        assertEquals(data.getImmediateCommitTimestamp(), 1699112309893478L);
        assertEquals(data.getOriginalCommitTimestamp(), 1699112309893478L);
        assertEquals(data.getTransactionLength(), 315);
        assertEquals(data.getImmediateServerVersion(), 80100);
        assertEquals(data.getOriginalServerVersion(), 80100);
        assertEquals(data.toString(), "GtidEventData{flags=0, gtid='bd9794e0-1d65-11ed-a7e7-0adb305b3a12:9', last_committed='7', sequence_number='8', immediate_commit_timestamp='1699112309893478', original_commit_timestamp='1699112309893478', transaction_length='315', immediate_server_version='80100', original_server_version='80100'}");
    }
}
