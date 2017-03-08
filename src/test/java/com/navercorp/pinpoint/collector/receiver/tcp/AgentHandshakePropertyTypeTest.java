package com.navercorp.pinpoint.collector.receiver.tcp;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by root on 17-1-10.
 */
public class AgentHandshakePropertyTypeTest {
    @Test
    public void should_return_false_when_geven_mock_properties_value_is_null() throws Exception {
        //given
        Map<Object, Object> properities = mock(Map.class);
        Object value = mock(Object.class);

        //when
        when(properities.get("hostname")).thenReturn(value);

        //then
        assertEquals(false, AgentHandshakePropertyType.hasAllType(properities));
    }

    @Test
    public void should_return_false_when_geven_mock_properties_value_is_Object_class() throws Exception {
        //given
        Map<Object, Object> properities = mock(Map.class);
        Object value = mock(Object.class);

        //when
        when(properities.get(anyString())).thenReturn(value);

        //then
        assertEquals(false, AgentHandshakePropertyType.hasAllType(properities));
    }
    @Test
    public void should_return_true_when_geven_mock_properties_value_is_Interger_class() throws Exception {
        //given
        Map<Object, Object> properities = mock(Map.class);

        //when
        when(properities.get("hostName")).thenReturn("localhost");
        when(properities.get("ip")).thenReturn("127.0.0.1");
        when(properities.get("agentId")).thenReturn("agent");
        when(properities.get("applicationName")).thenReturn("applicationName");
        when(properities.get("version")).thenReturn("version");
        when(properities.get("serviceType")).thenReturn(0);
        when(properities.get("pid")).thenReturn(0);
        when(properities.get("startTimestamp")).thenReturn(0L);

        //then
        assertEquals(true, AgentHandshakePropertyType.hasAllType(properities));
    }
}