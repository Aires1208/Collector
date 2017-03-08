package com.navercorp.pinpoint.collector.handler;

import com.navercorp.pinpoint.collector.dao.ApiMetaDataDao;
import com.navercorp.pinpoint.thrift.dto.TApiMetaData;
import com.navercorp.pinpoint.thrift.dto.TResult;
import org.apache.thrift.TBase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Created by root on 16-12-28.
 */
public class ApiMetaDataHandlerTest {

    @Mock
    private ApiMetaDataDao apiMetaDataDao;

    @Mock
    private Logger logger;

    @InjectMocks
    private ApiMetaDataHandler apiMetaDataHandler = new ApiMetaDataHandler();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void should_retrun_null_when_input_non_TApiMetaData_Object() throws Exception {
        //given
        TBase tBase = mock(TBase.class);

        //when
        TBase result = apiMetaDataHandler.handleRequest(tBase);

        //then
        assertNull(result);
        verify(apiMetaDataDao, times(0)).insert(any(TApiMetaData.class));
    }

    @Test
    public void should_retrun_success_when_input_ApiMetaData_Object() throws Exception {
        //given
        TBase tBase = mock(TApiMetaData.class);

        //when
        TBase result = apiMetaDataHandler.handleRequest(tBase);

        //then
        assertTrue(result instanceof TResult);
        assertTrue(((TResult)result).isSuccess());
        verify(apiMetaDataDao, times(1)).insert(any(TApiMetaData.class));
    }

    @Test
    public void should_retrun_false_when_input_TApiMetaData_Object() throws Exception {
        //given
        TBase tBase = mock(TApiMetaData.class);

        //when
        doThrow(new RuntimeException("test exception")).when(apiMetaDataDao).insert(any(TApiMetaData.class));
        TBase result = apiMetaDataHandler.handleRequest(tBase);

        //then
        assertTrue(result instanceof TResult);
        assertFalse(((TResult)result).isSuccess());
        verify(apiMetaDataDao, times(1)).insert(any(TApiMetaData.class));
    }
}