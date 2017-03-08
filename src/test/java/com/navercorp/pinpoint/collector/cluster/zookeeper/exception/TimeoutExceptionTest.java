package com.navercorp.pinpoint.collector.cluster.zookeeper.exception;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by root on 17-1-10.
 */
public class TimeoutExceptionTest {
    @Test
    public void testExcection() throws Exception {
        //given
        String message = "message";
        Throwable cause = new Throwable(message);

        //when
        TimeoutException exception0 = new TimeoutException();
        TimeoutException exception1 = new TimeoutException(message);
        TimeoutException exception2 = new TimeoutException(cause);
        TimeoutException exception3 = new TimeoutException(message, cause);

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