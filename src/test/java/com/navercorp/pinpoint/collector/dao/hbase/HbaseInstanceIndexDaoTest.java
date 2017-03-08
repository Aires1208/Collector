package com.navercorp.pinpoint.collector.dao.hbase;

import com.navercorp.pinpoint.collector.dao.InstanceIndexDao;
import com.navercorp.pinpoint.collector.dao.hbase.mapper.XLinkMapper;
import com.navercorp.pinpoint.collector.dao.hbase.mapper.XNodeMapper;
import com.navercorp.pinpoint.common.hbase.HBaseTables;
import com.navercorp.pinpoint.common.hbase.HbaseOperations2;
import com.navercorp.pinpoint.common.topo.domain.TopoLine;
import com.navercorp.pinpoint.common.topo.domain.XLink;
import com.navercorp.pinpoint.common.topo.domain.XNode;
import com.navercorp.pinpoint.common.trace.ServiceType;
import com.navercorp.pinpoint.common.util.RowKeyUtils;
import com.navercorp.pinpoint.common.util.TimeSlot;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HbaseInstanceIndexDaoTest {
    private static final String NODE_USER = "USER";
    private static final String NODE_CLIENT = "client_agent1";
    private static final String NODE_MAIN = "main_agent1";
    private static final String NODE_ORACLE = "Minos_oracle";

    @Mock
    private HbaseOperations2 hbaseTemplate;

    @Mock
    private TimeSlot timeSlot;

    @Mock
    private XNodeMapper xNodeMapper;

    @Mock
    private XLinkMapper xLinkMapper;

    @InjectMocks
    private InstanceIndexDao instanceIndexDao = new HbaseInstanceIndexDao();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void updateTopoLineTest() {
        //given
        String appName = "EMS";
        long timestamp = 1468585508533L;
        TopoLine topoLine = getTopoLine();
        byte[] rowkey = RowKeyUtils.createTimeSlotRowKey(appName, timestamp);

        //when
        when(timeSlot.getTimeSlot(anyLong())).thenReturn(timestamp);
        when(hbaseTemplate.get(HBaseTables.INSTANCEINDEX, rowkey, HBaseTables.INSTANCEINDEX_CF_N, xNodeMapper))
                .thenReturn(getxNodes());
        when(hbaseTemplate.get(HBaseTables.INSTANCEINDEX, rowkey, HBaseTables.INSTANCEINDEX_CF_L, xLinkMapper))
                .thenReturn(getxLinks());
        System.out.println("start");
        instanceIndexDao.update(appName, timestamp, topoLine);

        //then
        verify(hbaseTemplate).put(anyString(), any(Put.class));
        System.out.print("end");
    }

    private TopoLine getTopoLine() {
        List<XNode> xNodeList = getxNodes();
        List<XLink> xLinkList = getxLinks();
        return new TopoLine(xNodeList, xLinkList);
    }

    private List<XLink> getxLinks() {
        return newArrayList(new XLink(NODE_USER, NODE_CLIENT, 0, 0, 1),
                new XLink(NODE_CLIENT, NODE_MAIN, 300, 0, 1),
                new XLink(NODE_MAIN, NODE_ORACLE, 0, 1, 1));
    }

    private List<XNode> getxNodes() {
        return newArrayList(new XNode(ServiceType.USER.getName(), (short) 2, 0, 0, 1),
                new XNode(NODE_CLIENT, (short) 1000, 2000, 0, 1),
                new XNode(NODE_MAIN, (short) 1000, 1000, 1, 1),
                new XNode(NODE_ORACLE, (short) 2300, 0, 0, 1));
    }

}
