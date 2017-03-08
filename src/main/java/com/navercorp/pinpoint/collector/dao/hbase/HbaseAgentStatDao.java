/*
 * Copyright 2014 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.navercorp.pinpoint.collector.dao.hbase;

import static com.navercorp.pinpoint.common.hbase.HBaseTables.*;

import com.navercorp.pinpoint.collector.mapper.thrift.ActiveTraceHistogramBoMapper;
import com.navercorp.pinpoint.common.bo.ActiveTraceHistogramBo;
import com.navercorp.pinpoint.thrift.dto.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.AgentStatDao;
import com.navercorp.pinpoint.common.hbase.HbaseOperations2;
import com.navercorp.pinpoint.common.util.BytesUtils;
import com.navercorp.pinpoint.common.util.RowKeyUtils;
import com.navercorp.pinpoint.common.util.TimeUtils;
import com.sematext.hbase.wd.AbstractRowKeyDistributor;

/**
 * @author harebox
 * @author emeroad
 * @author HyunGil Jeong
 */
@Repository
public class HbaseAgentStatDao implements AgentStatDao {

    @Autowired
    private HbaseOperations2 hbaseTemplate;

    @Autowired
    @Qualifier("agentStatRowKeyDistributor")
    private AbstractRowKeyDistributor rowKeyDistributor;

    @Autowired
    private ActiveTraceHistogramBoMapper activeTraceHistogramBoMapper;

    @Override
    public void insert(final TAgentStat agentStat) {
        if (agentStat == null) {
            throw new NullPointerException("agentStat must not be null");
        }
        Put put = createPut(agentStat);
        hbaseTemplate.put(AGENT_STAT, put);
    }

    private Put createPut(TAgentStat agentStat) {
        long timestamp = agentStat.getTimestamp();
        byte[] key = getDistributedRowKey(agentStat, timestamp);

        Put put = new Put(key);

        final long collectInterval = agentStat.getCollectInterval();
        put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_INTERVAL, Bytes.toBytes(collectInterval));
        // GC, JVM_HEAP_MEM
        if (agentStat.isSetGc()) {
            TJvmGc gc = agentStat.getGc();
            put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_GC_TYPE, Bytes.toBytes(gc.getType().name()));
            put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_GC_OLD_COUNT, Bytes.toBytes(gc.getJvmGcOldCount()));
            put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_GC_OLD_TIME, Bytes.toBytes(gc.getJvmGcOldTime()));
            put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_HEAP_USED, Bytes.toBytes(gc.getJvmMemoryHeapUsed()));
            put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_HEAP_MAX, Bytes.toBytes(gc.getJvmMemoryHeapMax()));
            put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_NON_HEAP_USED, Bytes.toBytes(gc.getJvmMemoryNonHeapUsed()));
            put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_NON_HEAP_MAX, Bytes.toBytes(gc.getJvmMemoryNonHeapMax()));
            if (gc.isSetJvmGcDetailed()) {
                TJvmGcDetailed jvmGcDetailed = gc.getJvmGcDetailed();
                put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_GC_NEW_COUNT, Bytes.toBytes(jvmGcDetailed.getJvmGcNewCount()));
                put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_GC_NEW_TIME, Bytes.toBytes(jvmGcDetailed.getJvmGcNewTime()));
                put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_GC_POOL_CODECACHE_UESD, Bytes.toBytes(jvmGcDetailed.getJvmPoolCodeCacheUsed()));
                put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_GC_POOL_NEWGEN_USED, Bytes.toBytes(jvmGcDetailed.getJvmPoolNewGenUsed()));
                put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_GC_POOL_OLDGEN_USED, Bytes.toBytes(jvmGcDetailed.getJvmPoolOldGenUsed()));
                put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_GC_POOL_SURVIVOR_USED, Bytes.toBytes(jvmGcDetailed.getJvmPoolSurvivorSpaceUsed()));
                put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_GC_POOL_PERMGEN_USED, Bytes.toBytes(jvmGcDetailed.getJvmPoolPermGenUsed()));
                put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_GC_POOL_METASPACE_USED, Bytes.toBytes(jvmGcDetailed.getJvmPoolMetaspaceUsed()));
            }
        } else {
            put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_GC_TYPE, Bytes.toBytes(TJvmGcType.UNKNOWN.name()));
        }
        // CPU
        if (agentStat.isSetCpuLoad()) {
            TCpuLoad cpuLoad = agentStat.getCpuLoad();
            put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_JVM_CPU, Bytes.toBytes(cpuLoad.getJvmCpuLoad()));
            put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_SYS_CPU, Bytes.toBytes(cpuLoad.getSystemCpuLoad()));
        }
        //MEM
        if (agentStat.isSetMemLoad()) {
            TMemLoad memLoad = agentStat.getMemLoad();
            put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_MEM_TOTAL, Bytes.toBytes(memLoad.getTotal()));
            put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_MEM_FREE, Bytes.toBytes(memLoad.getFree()));
            put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_MEM_USED, Bytes.toBytes(memLoad.getUsed()));
        }
        // IO
        if (agentStat.isSetIOLoad()) {
            TIOLoad tioLoad = agentStat.getIOLoad();
            put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_DISK_TOTAL, Bytes.toBytes(tioLoad.getTotal()));
            put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_DISK_FREE, Bytes.toBytes(tioLoad.getFree()));
            put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_DISK_USED, Bytes.toBytes(tioLoad.getUsed()));
            put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_DISK_USAGE, Bytes.toBytes(tioLoad.getUsage()));
        }
        // NETWORK
        if (agentStat.isSetNetLoad()) {
            TNetLoad tNetLoad = agentStat.getNetLoad();
            put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_NET_DOWN_SPEEF, Bytes.toBytes(tNetLoad.getInSpeed()));
            put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_NET_UP_SPEED, Bytes.toBytes(tNetLoad.getOutSpeed()));
            put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_NET_TOTAL_SPEED, Bytes.toBytes(tNetLoad.getSpeed()));
        }
        // Transaction
        if (agentStat.isSetTransaction()) {
            TTransaction transaction = agentStat.getTransaction();
            put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_TRANSACTION_SAMPLED_NEW, Bytes.toBytes(transaction.getSampledNewCount()));
            put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_TRANSACTION_SAMPLED_CONTINUATION, Bytes.toBytes(transaction.getSampledContinuationCount()));
            put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_TRANSACTION_UNSAMPLED_NEW, Bytes.toBytes(transaction.getUnsampledNewCount()));
            put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_TRANSACTION_UNSAMPLED_CONTINUATION, Bytes.toBytes(transaction.getUnsampledContinuationCount()));
        }
        // Active Trace
        if (agentStat.isSetActiveTrace()) {
            TActiveTrace activeTrace = agentStat.getActiveTrace();
            if (activeTrace.isSetHistogram()) {
                ActiveTraceHistogramBo activeTraceHistogramBo = this.activeTraceHistogramBoMapper.map(activeTrace.getHistogram());
                put.addColumn(AGENT_STAT_CF_STATISTICS, AGENT_STAT_COL_ACTIVE_TRACE_HISTOGRAM, activeTraceHistogramBo.writeValue());
            }
        }
        return put;
    }

    /**
     * Create row key based on the timestamp
     */
    private byte[] getRowKey(String agentId, long timestamp) {
        if (agentId == null) {
            throw new IllegalArgumentException("agentId must not null");
        }
        byte[] bAgentId = BytesUtils.toBytes(agentId);
        return RowKeyUtils.concatFixedByteAndLong(bAgentId, AGENT_NAME_MAX_LEN, TimeUtils.reverseTimeMillis(timestamp));
    }

    /**
     * Create row key based on the timestamp and distribute it into different buckets
     */
    private byte[] getDistributedRowKey(TAgentStat agentStat, long timestamp) {
        byte[] key = getRowKey(agentStat.getAgentId(), timestamp);
        return rowKeyDistributor.getDistributedKey(key);
    }

}
