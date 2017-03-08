package com.navercorp.pinpoint.collector.manage.jmx;

import com.navercorp.pinpoint.collector.manage.ClusterManager;
import com.navercorp.pinpoint.collector.manage.CollectorManager;
import com.navercorp.pinpoint.collector.manage.HandlerManager;
import com.navercorp.pinpoint.rpc.util.ListUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

/**
 * Created by root on 16-12-28.
 */
public class JMXCollectorManagerTest {

    @Mock
    private HandlerManager handlerManager;

    @Mock
    private ClusterManager clusterManager;

    @Mock
    private JMXCollectorManagerList jmxCollectorManagerList;

    @InjectMocks
    private JMXCollectorManager jmxCollectorManager = new JMXCollectorManager();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        List<CollectorManager> supportManagerList = newArrayList();
        ListUtils.addIfValueNotNull(supportManagerList, handlerManager);
        when(jmxCollectorManagerList.getSupportList()).thenReturn(supportManagerList);

        when(handlerManager.getName()).thenReturn("test");
    }

    @Test
    public void getMBean() throws Exception {
        //given
        String name = "test";

        //when
        jmxCollectorManager.setUp();
        CollectorManager collectorManager = jmxCollectorManager.getMBean(name);

        //then
        assertNotNull(collectorManager);
        jmxCollectorManager.tearDown();
    }

}