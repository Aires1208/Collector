package com.navercorp.pinpoint.collector.manage;

import com.navercorp.pinpoint.collector.cluster.ClusterPointLocator;
import com.navercorp.pinpoint.collector.cluster.TargetClusterPoint;
import com.navercorp.pinpoint.collector.config.CollectorConfiguration;
import org.junit.Test;
import org.omg.CORBA.Object;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by root on 16-12-28.
 */
public class ClusterManagerTest {

    @Test
    public void testenableCluster() throws Exception {
        //given
        CollectorConfiguration collectorConfiguration = mock(CollectorConfiguration.class);
        ClusterPointLocator clusterPointLocator = mock(ClusterPointLocator.class);

        //when
        when(collectorConfiguration.isClusterEnable()).thenReturn(true);
        ClusterManager manager = new ClusterManager(collectorConfiguration, clusterPointLocator);

        boolean isEnableCluster = manager.isEnable();

        //then
        assertTrue(isEnableCluster);
        assertThat(manager.getName(), is("ClusterManager"));
    }

    @Test
    public void getConnectedAgentList() throws Exception {
        //given
        CollectorConfiguration collectorConfiguration = mock(CollectorConfiguration.class);
        ClusterPointLocator clusterPointLocator = mock(ClusterPointLocator.class);

//        Object clusterPoint = mock(Object.class);
        TargetClusterPoint agentClusterPoint = mock(TargetClusterPoint.class);

        //when
        when(agentClusterPoint.getAgentId()).thenReturn("fm-agent");
        when(agentClusterPoint.gerVersion()).thenReturn("1.5.2");
        when(agentClusterPoint.getApplicationName()).thenReturn("fm_active");
        when(agentClusterPoint.getStartTimeStamp()).thenReturn(12L);

        when(clusterPointLocator.getClusterPointList()).thenReturn(newArrayList(agentClusterPoint));
        ClusterManager manager = new ClusterManager(collectorConfiguration, clusterPointLocator);

        List<String> connectedAgentList = manager.getConnectedAgentList();

        //then
        assertEquals(1, connectedAgentList.size());
        assertEquals("fm_active/fm-agent/12", connectedAgentList.get(0));
    }

}