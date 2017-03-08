package com.navercorp.pinpoint.collector.dao.hbase;

import com.navercorp.pinpoint.collector.dao.RpcStatisticDao;
import com.navercorp.pinpoint.collector.dao.hbase.mapper.XRpcMapper;
import com.navercorp.pinpoint.common.hbase.HbaseOperations2;
import com.navercorp.pinpoint.common.topo.domain.XRpc;
import com.navercorp.pinpoint.common.topo.domain.XRpcBuilder;
import com.navercorp.pinpoint.common.util.TimeSlot;
import com.navercorp.pinpoint.thrift.dto.TSpan;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HbaseRpcStatisticDaoTest {
    @Mock
    private TimeSlot timeSlot;

    @Mock
    private HbaseOperations2 hbaseTemplate;

    @Mock
    private XRpcMapper xRpcMapper;

    @InjectMocks
    private RpcStatisticDao rpcStatisticDao = new HbaseRpcStatisticDao();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shoule_run_mergerTSpanToRpc_method() throws Exception {
        //given
        TSpan tSpan = getSpan();
        XRpc rpc = buildRpc();

        //when
        when(this.hbaseTemplate.get(anyString(), any(byte[].class),
                any(byte[].class), any(byte[].class), any(XRpcMapper.class))).thenReturn(rpc);
        rpcStatisticDao.update(tSpan);

        //then
        verify(hbaseTemplate).put(anyString(), any(Put.class));
    }

    @Test
    public void shoule_run_fetchXRpcByTSpan_method() throws Exception {
        //given
        TSpan tSpan = getSpan();

        //when
        rpcStatisticDao.update(tSpan);

        //then
        verify(hbaseTemplate).put(anyString(), any(Put.class));
    }


    private XRpc buildRpc() {
        XRpcBuilder builder = new XRpcBuilder();
        builder.Rpc("test/a/v");
        builder.Method("POST");
        builder.Count(3);
        builder.Duration(203);
        builder.AvgTime(332);
        builder.SuccessCount(2);
        builder.MaxTime(400);
        builder.MinTime(173);

        return builder.build();
    }

    private TSpan getSpan() {
        TSpan tSpan = new TSpan();

        tSpan.setApplicationName("APP=EMS");
        tSpan.setRpc("test/a/v");
        tSpan.setElapsed(122333);
        tSpan.setStartTime(1479947700000l);
        tSpan.setErr(0);
        tSpan.setRestControlName("POST");

        return tSpan;
    }

}