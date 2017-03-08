package com.navercorp.pinpoint.collector.cluster.zookeeper.exception;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by root on 17-1-10.
 */
public class AuthExceptionTest {
    @Test
    public void testException() throws Exception {
        //given
        String message = "message";
        Throwable cause = new Throwable(message);

        //when
        AuthException exception0 = new AuthException();
        AuthException exception1 = new AuthException(message);
        AuthException exception2 = new AuthException(cause);
        AuthException exception3 = new AuthException(message, cause);

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