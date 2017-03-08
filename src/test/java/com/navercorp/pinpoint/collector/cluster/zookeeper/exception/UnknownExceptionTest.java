package com.navercorp.pinpoint.collector.cluster.zookeeper.exception;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by root on 17-1-10.
 */
public class UnknownExceptionTest {
    @Test
    public void testException() throws Exception {
        //given
        String message = "message";
        Throwable cause = new Throwable(message);

        //when
        UnknownException exception0 = new UnknownException();
        UnknownException exception1 = new UnknownException(message);
        UnknownException exception2 = new UnknownException(cause);
        UnknownException exception3 = new UnknownException(message, cause);

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