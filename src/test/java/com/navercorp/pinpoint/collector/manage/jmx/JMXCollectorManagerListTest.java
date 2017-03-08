package com.navercorp.pinpoint.collector.manage.jmx;

import com.navercorp.pinpoint.collector.manage.ClusterManager;
import com.navercorp.pinpoint.collector.manage.CollectorManager;
import com.navercorp.pinpoint.collector.manage.HandlerManager;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by root on 16-12-28.
 */
public class JMXCollectorManagerListTest {

    @Mock
    private HandlerManager handlerManager;

    @Mock
    private ClusterManager clusterManager;

    @InjectMocks
    private JMXCollectorManagerList jmxCollectorManagerList = new JMXCollectorManagerList();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void should_return_empty_list() throws Exception {
        //given

        //when
        List<CollectorManager> collectorManagers = jmxCollectorManagerList.getSupportList();

        //then
        assertTrue(collectorManagers.isEmpty());
    }

    @Test
    public void should_return_CollectorManager_list_size_2() throws Exception {
        //given
        Field isActive = jmxCollectorManagerList.getClass().getDeclaredField("isActive");
        isActive.setAccessible(true);
        isActive.setBoolean(jmxCollectorManagerList, true);

        //when
        List<CollectorManager> collectorManagers = jmxCollectorManagerList.getSupportList();

        //then
        assertEquals(2, collectorManagers.size());
    }
}