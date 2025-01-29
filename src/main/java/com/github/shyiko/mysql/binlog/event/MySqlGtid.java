package com.github.shyiko.mysql.binlog.event;

import java.util.UUID;

public class MySqlGtid {
    private final UUID serverId;
    private final long transactionId;

    public MySqlGtid(UUID serverId, long transactionId) {
        this.serverId = serverId;
        this.transactionId = transactionId;
    }

    public static MySqlGtid fromString(String gtid) {
        int separatorPos = gtid.indexOf(":");
        // Consider switching to Long.parseLong(CharSequence, int, int, int) after upgrading to JDK 9 or above
        // for a substantial performance boost.
        long transactionId = Long.parseLong(gtid.substring(separatorPos + 1));
        return new MySqlGtid(UUID.fromString(gtid.substring(0, separatorPos)), transactionId);
    }

    @Override
    public String toString() {
        return serverId.toString()+":"+transactionId;
    }

    public UUID getServerId() {
        return serverId;
    }

    public long getTransactionId() {
        return transactionId;
    }
}
