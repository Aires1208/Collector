package com.navercorp.pinpoint.collector.handler;

import com.navercorp.pinpoint.collector.dao.StringMetaDataDao;
import com.navercorp.pinpoint.thrift.dto.TResult;
import com.navercorp.pinpoint.thrift.dto.TStringMetaData;
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
public class StringMetaDataHandlerTest {

    @Mock
    private Logger logger;

    @Mock
    private StringMetaDataDao stringMetaDataDao;

    @InjectMocks
    private StringMetaDataHandler handler = new StringMetaDataHandler();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void should_return_null_when_input_non_TStringMetaData() throws Exception {
        //given
        TBase tBase = mock(TBase.class);

        //when
        TBase result = handler.handleRequest(tBase);

        //then
        assertNull(result);
        verify(stringMetaDataDao, times(0)).insert(any(TStringMetaData.class));
    }

    @Test
    public void should_return_success_when_input_TStringMetaData_object() throws Exception {
        //given
        TBase tBase = mock(TStringMetaData.class);

        //when
        when(logger.isDebugEnabled()).thenReturn(true);
        TBase result = handler.handleRequest(tBase);

        //then
        assertTrue(result instanceof TResult);
        assertTrue(((TResult) result).isSuccess());
        verify(stringMetaDataDao, times(1)).insert(any(TStringMetaData.class));
    }

    @Test
    public void should_return_false_when_input_TStringMetaData_object() throws Exception {
        //given
        TBase tBase = mock(TStringMetaData.class);

        //when
        doThrow(new RuntimeException("test exception")).when(stringMetaDataDao).insert(any(TStringMetaData.class));
        TBase result = handler.handleRequest(tBase);

        //then
        assertTrue(result instanceof TResult);
        assertFalse(((TResult) result).isSuccess());
        verify(stringMetaDataDao, times(1)).insert(any(TStringMetaData.class));
    }

}