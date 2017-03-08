package com.navercorp.pinpoint.collector.cluster.zookeeper.exception;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by root on 17-1-10.
 */
public class ConnectionExceptionTest {
    @Test
    public void testException() throws Exception {
        //given
        String message = "message";
        Throwable cause = new Throwable(message);

        //when
        ConnectionException exception0 = new ConnectionException();
        ConnectionException exception1 = new ConnectionException(message);
        ConnectionException exception2 = new ConnectionException(cause);
        ConnectionException exception3 = new ConnectionException(message, cause);

        //then
        assertNull(exception0.getMessage());
        assertNull(exception0.getCause());
        assertEquals(exception1.getMessage(), message);
        assertNull(exception1.getCause());
        assertEquals(exception2.getCause(), cause);
        assertNotNull(exception2.getMessage());
        assertEquals(message, exception3.getMessage());
        assertEquals(cause, exception3.getCause());
    }
}