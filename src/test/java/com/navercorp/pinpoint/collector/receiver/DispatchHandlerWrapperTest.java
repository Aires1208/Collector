package com.navercorp.pinpoint.collector.receiver;

import com.navercorp.pinpoint.collector.manage.HandlerManager;
import com.navercorp.pinpoint.thrift.dto.TResult;
import org.apache.thrift.TBase;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Created by root on 16-12-28.
 */
public class DispatchHandlerWrapperTest {

    @Test
    public void dispatchSendMessage() throws Exception {
        //given
        DispatchHandler dispatchHandler = mock(DispatchHandler.class);
        DispatchHandlerWrapper dispatchHandlerWrapper = new DispatchHandlerWrapper(dispatchHandler);
        TBase tBase = mock(TBase.class);

        //when
        dispatchHandlerWrapper.dispatchSendMessage(tBase);

        //then
        verify(dispatchHandler).dispatchSendMessage(any(TBase.class));
    }

    @Test
    public void should_dispatchRequestMessage() throws Exception {
        //given
        DispatchHandler dispatchHandler = mock(DispatchHandler.class);
        DispatchHandlerWrapper dispatchHandlerWrapper = new DispatchHandlerWrapper(dispatchHandler);
        TBase tBase = mock(TBase.class);

        //when
        TBase result = dispatchHandlerWrapper.dispatchRequestMessage(tBase);

        //then
        verify(dispatchHandler).dispatchRequestMessage(any(TBase.class));
    }

    @Test
    public void should_return_false_when_dispatchRequestMessage() throws Exception {
        //given
        DispatchHandler dispatchHandler = mock(DispatchHandler.class);
        DispatchHandlerWrapper dispatchHandlerWrapper = new DispatchHandlerWrapper(dispatchHandler);
        HandlerManager handlerManager = mock(HandlerManager.class);
        setHandlerManager(dispatchHandlerWrapper, handlerManager);
        TBase tBase = mock(TBase.class);

        //when
        when(handlerManager.isEnable()).thenReturn(false);
        TBase result = dispatchHandlerWrapper.dispatchRequestMessage(tBase);

        //then
        verify(dispatchHandler, times(0)).dispatchRequestMessage(any(TBase.class));
        assertTrue(result instanceof TResult);
        assertEquals(false,((TResult) result).isSuccess());
    }

    private void setHandlerManager(DispatchHandlerWrapper dispatchHandlerWrapper, HandlerManager handlerManager) throws NoSuchFieldException, IllegalAccessException {
        Field manager = dispatchHandlerWrapper.getClass().getDeclaredField("handlerManager");
        manager.setAccessible(true);
        manager.set(dispatchHandlerWrapper, handlerManager);
    }
}