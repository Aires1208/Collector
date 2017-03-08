package com.navercorp.pinpoint.collector.dao.hbase;

import com.navercorp.pinpoint.collector.dao.AgentStatDao;
import com.navercorp.pinpoint.collector.mapper.thrift.ActiveTraceHistogramBoMapper;
import com.navercorp.pinpoint.common.bo.ActiveTraceHistogramBo;
import com.navercorp.pinpoint.common.hbase.HbaseOperations2;
import com.navercorp.pinpoint.thrift.dto.*;
import com.sematext.hbase.wd.AbstractRowKeyDistributor;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

import static com.google.common.collect.Lists.newArrayList;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HbaseAgentStatTest {
    @Mock
    private HbaseOperations2 hbaseTemplate;

    @Mock
    private AbstractRowKeyDistributor rowKeyDistributor;

    @Mock
    private ActiveTraceHistogramBoMapper activeTraceHistogramBoMapper;

    @InjectMocks
    private AgentStatDao agentStatDao = new HbaseAgentStatDao();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testInsert() {
        //given
        TAgentStat agentStat = setAgentStat();

        //when
        when(this.rowKeyDistributor.getDistributedKey(any(byte[].class))).thenReturn(new byte[]{1});
        when(this.activeTraceHistogramBoMapper.map(gettActiveTraceHistogram())).thenReturn(buildHistogramBo());
        agentStatDao.insert(agentStat);

        //then
        verify(hbaseTemplate).put(anyString(), any(Put.class));
        System.out.println("end");
    }

    private ActiveTraceHistogramBo buildHistogramBo() {
        return new ActiveTraceHistogramBo(0, 1, newArrayList(1, 2, 2, 5));
    }

    private TAgentStat setAgentStat() {
        TAgentStat agentStat = new TAgentStat();
        agentStat.setAgentId("test_agentforgc");
        agentStat.setTimestamp(System.currentTimeMillis());
        agentStat.setGc(setGc());
        agentStat.setCpuLoad(setCpuLaod());
        agentStat.setIOLoad(setIOLoad());
        agentStat.setMemLoad(setMemLoad());
        agentStat.setNetLoad(setNetNLoad());
        agentStat.setTransaction(setTransaction());
        agentStat.setActiveTrace(setActiveTrace());
        return agentStat;
    }

    private TActiveTrace setActiveTrace() {
        TActiveTrace activeTrace = new TActiveTrace();

        TActiveTraceHistogram histogram = gettActiveTraceHistogram();

        activeTrace.setHistogram(histogram);

        return activeTrace;
    }

    private TActiveTraceHistogram gettActiveTraceHistogram() {
        TActiveTraceHistogram histogram = new TActiveTraceHistogram((short) 0);
        histogram.setHistogramSchemaType(1);
        histogram.setActiveTraceCount(newArrayList(1, 2, 2, 5));
        return histogram;
    }

    private TTransaction setTransaction() {
        TTransaction tTransaction = new TTransaction();
        tTransaction.setSampledNewCount(1L);
        tTransaction.setUnsampledNewCount(2L);
        tTransaction.setSampledContinuationCount(3L);
        tTransaction.setUnsampledContinuationCount(4L);
        return tTransaction;
    }

    private TNetLoad setNetNLoad() {
        TNetLoad tNetLoad = new TNetLoad();
        tNetLoad.setInSpeed(43.55);
        tNetLoad.setOutSpeed(10.03);
        tNetLoad.setSpeed(1024 * 1024 * 100L);

        return tNetLoad;
    }

    private TMemLoad setMemLoad() {
        TMemLoad tMemLoad = new TMemLoad();
        tMemLoad.setTotal(8010840L);
        tMemLoad.setFree(1084208L);
        tMemLoad.setUsed(6306416L);
        return tMemLoad;
    }

    private TIOLoad setIOLoad() {
        TIOLoad tioLoad = new TIOLoad();
        tioLoad.setTotal(76765216L);
        tioLoad.setFree(60093128L);
        tioLoad.setUsed(12749504L);
        tioLoad.setUsage(0.18);

        return tioLoad;
    }

    private TCpuLoad setCpuLaod() {
        TCpuLoad tcpuLoad = new TCpuLoad();
        tcpuLoad.setJvmCpuLoad(0.0032);
        tcpuLoad.setSystemCpuLoad(0.0054);

        return tcpuLoad;
    }

    private TJvmGc setGc() {
        TJvmGc gc = new TJvmGc();
        gc.setType(TJvmGcType.PARALLEL);
        gc.setJvmGcOldCount(1);
        gc.setJvmGcOldTime(100);
        gc.setJvmMemoryHeapUsed(222764512L);
        gc.setJvmMemoryHeapMax(1823473664L);
        gc.setJvmMemoryNonHeapUsed(66328816L);
        gc.setJvmMemoryNonHeapMax(224395264L);
        gc.setJvmGcDetailed(setJvmDetailed());

        return gc;
    }


    private TJvmGcDetailed setJvmDetailed() {
        TJvmGcDetailed jvmGcDetailed = new TJvmGcDetailed();
        jvmGcDetailed.setJvmGcNewCount(23456L);
        jvmGcDetailed.setJvmGcNewTime(567890L);
        jvmGcDetailed.setJvmPoolNewGenUsed(0.86);
        jvmGcDetailed.setJvmPoolOldGenUsed(1.66);
        jvmGcDetailed.setJvmPoolCodeCacheUsed(0.64);
        jvmGcDetailed.setJvmPoolSurvivorSpaceUsed(3.98);
        jvmGcDetailed.setJvmPoolPermGenUsed(2.55);
        jvmGcDetailed.setJvmPoolMetaspaceUsed(1.45);

        return jvmGcDetailed;
    }

}
