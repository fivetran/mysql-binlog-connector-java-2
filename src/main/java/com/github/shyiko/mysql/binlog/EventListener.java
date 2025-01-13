package com.github.shyiko.mysql.binlog;

import com.github.shyiko.mysql.binlog.event.Event;

/**
 * {@link BinaryLogClient}'s event listener.
 */
public interface EventListener {

    void onEvent(Event event);
}
