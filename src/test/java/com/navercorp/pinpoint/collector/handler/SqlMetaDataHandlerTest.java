package com.navercorp.pinpoint.collector.handler;

import com.navercorp.pinpoint.collector.dao.SqlMetaDataDao;
import com.navercorp.pinpoint.thrift.dto.TResult;
import com.navercorp.pinpoint.thrift.dto.TSqlMetaData;
import org.apache.thrift.TBase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Created by root on 16-12-28.
 */
public class SqlMetaDataHandlerTest {

    @Mock
    private Logger logger;

    @Mock
    private SqlMetaDataDao sqlMetaDataDao;

    @InjectMocks
    private SqlMetaDataHandler sqlMetaDataHandler = new SqlMetaDataHandler();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void should_return_null_when_input_non_TSqlMetaData() throws Exception {
        //given
        TBase tBase = mock(TBase.class);

        //when
        TBase result = sqlMetaDataHandler.handleRequest(tBase);

        //then
        assertNull(result);
        verify(sqlMetaDataDao, times(0)).insert(any(TSqlMetaData.class));
    }

    @Test
    public void should_return_success_when_input_TSqlMetaData_object() throws Exception {
        //given
        TBase tBase = mock(TSqlMetaData.class);

        //when
        when(logger.isDebugEnabled()).thenReturn(true);
        TBase result = sqlMetaDataHandler.handleRequest(tBase);

        //then
        assertTrue(result instanceof TResult);
        assertTrue(((TResult)result).isSuccess());
        verify(sqlMetaDataDao, times(1)).insert(any(TSqlMetaData.class));
    }

    @Test
    public void should_return_false_when_input_TSqlMetaData_object() throws Exception {
        //given
        TBase tBase = mock(TSqlMetaData.class);

        //when
        doThrow(new RuntimeException("test exception")).when(sqlMetaDataDao).insert(any(TSqlMetaData.class));
        TBase result = sqlMetaDataHandler.handleRequest(tBase);

        //then
        assertTrue(result instanceof TResult);
        assertFalse(((TResult)result).isSuccess());
        verify(sqlMetaDataDao, times(1)).insert(any(TSqlMetaData.class));
    }
}