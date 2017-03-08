package com.navercorp.pinpoint.collector.util;

import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Matchers.anyLong;

/**
 * Created by root on 16-12-22.
 */
public class ThreadLocalAcceptedTimeServiceTest {

    @InjectMocks
    private AcceptedTimeService acceptedTimeService = new ThreadLocalAcceptedTimeService();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void accept() throws Exception {
        //given
        long timestamp = System.currentTimeMillis();

        //then
        acceptedTimeService.accept();

        //then
        System.out.println(acceptedTimeService.getAcceptedTime());
//        Mockito.verify(acceptedTimeService).accept(anyLong());
    }

    @Test
    public void accept1() throws Exception {
        //given
        long timestamp = System.currentTimeMillis();

        //when
        acceptedTimeService.accept(timestamp);

        //then
        assertEquals(timestamp, acceptedTimeService.getAcceptedTime());
    }

}