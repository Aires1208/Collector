package com.navercorp.pinpoint.collector.dao.hbase;

import com.google.common.base.Preconditions;
import com.navercorp.pinpoint.collector.dao.InstanceIndexDao;
import com.navercorp.pinpoint.collector.dao.hbase.mapper.XLinkMapper;
import com.navercorp.pinpoint.collector.dao.hbase.mapper.XNodeMapper;
import com.navercorp.pinpoint.collector.usercase.CalculateTopoLineMergeUserCase;
import com.navercorp.pinpoint.common.buffer.AutomaticBuffer;
import com.navercorp.pinpoint.common.buffer.Buffer;
import com.navercorp.pinpoint.common.hbase.HBaseTables;
import com.navercorp.pinpoint.common.hbase.HbaseOperations2;
import com.navercorp.pinpoint.common.topo.domain.TopoLine;
import com.navercorp.pinpoint.common.topo.domain.XLink;
import com.navercorp.pinpoint.common.topo.domain.XNode;
import com.navercorp.pinpoint.common.util.BytesUtils;
import com.navercorp.pinpoint.common.util.RowKeyUtils;
import com.navercorp.pinpoint.common.util.TimeSlot;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class HbaseInstanceIndexDao implements InstanceIndexDao {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final Object syncObject = new Object();

    @Autowired
    private HbaseOperations2 hbaseTemplate;

    @Autowired
    private TimeSlot timeSlot;

    @Autowired
    private XNodeMapper xNodeMapper;

    @Autowired
    private XLinkMapper xLinkMapper;

    @Override
    public void update(String appName, long timestamp, TopoLine topoLine) {
        Preconditions.checkArgument(appName != null, new NullPointerException("appName must not be empty"));
        Preconditions.checkArgument(topoLine != null, new NullPointerException("tracetopo must not be empty"));

        long timeslot = timeSlot.getTimeSlot(timestamp);
        byte[] rowKey = RowKeyUtils.createTimeSlotRowKey(appName, timeslot);

        List<XNode> nodeList = hbaseTemplate.get(HBaseTables.INSTANCEINDEX, rowKey, HBaseTables.INSTANCEINDEX_CF_N, xNodeMapper);
        List<XLink> linkList = hbaseTemplate.get(HBaseTables.INSTANCEINDEX, rowKey, HBaseTables.INSTANCEINDEX_CF_L, xLinkMapper);

        TopoLine tmpTopoline = new TopoLine(nodeList, linkList);
        CalculateTopoLineMergeUserCase userCase = new CalculateTopoLineMergeUserCase(tmpTopoline, topoLine);
        if (!(nodeList.isEmpty() || linkList.isEmpty())) {
            tmpTopoline = userCase.execute();
            logger.info("merge topoline, appName={}, topo={}, timeslot={}", appName, tmpTopoline.toString(), timeslot);
        } else {
            tmpTopoline = topoLine;
            logger.info("add new record for topoline, appName={}, topo={}, timeslot={}", appName, tmpTopoline.toString(), timeslot);
        }

        Put put = new Put(rowKey);
        for (XNode xNode : tmpTopoline.getXNodes()) {
            put.addColumn(HBaseTables.INSTANCEINDEX_CF_N, BytesUtils.toBytes(xNode.getName()), xNode.writeValue());
        }

        for (XLink xLink : tmpTopoline.getXLinks()) {
            byte[] qualifier = createLinkQualifier(xLink.getFrom(), xLink.getTo());
            put.addColumn(HBaseTables.INSTANCEINDEX_CF_L, qualifier, xLink.writeValue());
        }

        //insert new data
        synchronized (syncObject) {
            hbaseTemplate.put(HBaseTables.INSTANCEINDEX, put);
        }
    }

    private byte[] createLinkQualifier(String from, String to) {
        final Buffer buffer = new AutomaticBuffer();
        buffer.putPrefixedString(from);
        buffer.putPrefixedString(to);
        return buffer.getBuffer();
    }

}
