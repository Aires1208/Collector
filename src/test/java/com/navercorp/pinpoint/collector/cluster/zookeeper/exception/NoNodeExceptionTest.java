package com.navercorp.pinpoint.collector.cluster.zookeeper.exception;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by root on 17-1-10.
 */
public class NoNodeExceptionTest {
    @Test
    public void testException() throws Exception {
        //given
        String message = "message";
        Throwable cause = new Throwable(message);

        //when
        NoNodeException exception0 = new NoNodeException();
        NoNodeException exception1 = new NoNodeException(message);
        NoNodeException exception2 = new NoNodeException(cause);
        NoNodeException exception3 = new NoNodeException(message, cause);

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