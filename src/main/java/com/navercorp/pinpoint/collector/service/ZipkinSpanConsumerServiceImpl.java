package com.navercorp.pinpoint.collector.service;

import com.navercorp.pinpoint.collector.dao.*;
import com.navercorp.pinpoint.collector.eventbus.TransactionEventKey;
import com.navercorp.pinpoint.collector.eventbus.TransactionEventValue;
import com.navercorp.pinpoint.collector.usercase.Converter;
import com.navercorp.pinpoint.common.util.TransactionId;
import com.navercorp.pinpoint.common.util.TransactionIdUtils;
import com.navercorp.pinpoint.thrift.dto.TAgentInfo;
import com.navercorp.pinpoint.thrift.dto.TApiMetaData;
import com.navercorp.pinpoint.thrift.dto.TSpan;
import com.navercorp.pinpoint.thrift.dto.TSqlMetaData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import zipkin.Span;

import java.util.List;

/**
 * Created by root on 16-12-8.
 */
@Service
public class ZipkinSpanConsumerServiceImpl implements ZipkinSpanConsumerService {

    @Autowired
    private ApplicationTraceIndexDao applicationTraceIndexDao;

    @Autowired
    private TracesDao tracesDao;

    @Autowired
    private TransactionListDao transactionListDao;

    @Autowired
    private AgentInfoDao agentInfoDao;

    @Autowired
    private ApplicationIndexDao applicationIndexDao;

    @Autowired
    private ServiceTraceIdIndexDao serviceTraceIdIndexDao;

    @Autowired
    private InstanceTraceIdIndexDao instanceTraceIdIndexDao;

    @Autowired
    private ApiMetaDataDao apiMetaDataDao;

    @Autowired
    private TopoService topoService;

    @Autowired
    private SqlMetaDataDao sqlMetaDataDao;

    @Override
    public void consume(List<Span> spans) {
        Converter converter = new Converter(spans);

        //1,Trace data
        List<TSpan> pinpointSpans = converter.convert();
        for (TSpan tSpan : pinpointSpans) {
            tracesDao.insert(tSpan);
            applicationTraceIndexDao.insert(tSpan);
            transactionListDao.insert(tSpan);

            if (tSpan.getParentSpanId() == -1L) {
                serviceTraceIdIndexDao.update(tSpan);
                instanceTraceIdIndexDao.update(tSpan);
            }
        }

        //2, sql/api data
        List<TApiMetaData> apis = converter.buildApiInfo();
        for (TApiMetaData apiMetaData : apis) {
            apiMetaDataDao.insert(apiMetaData);
        }

        List<TSqlMetaData> sqls = converter.buildSqlInfo();
        if (!sqls.isEmpty()) {
            for (TSqlMetaData sqlMetaData : sqls) {
                sqlMetaDataDao.insert(sqlMetaData);
            }
        }

        //3,AgentInfo data
        List<TAgentInfo> agentInfos = converter.buildAgentInfos();
        for (TAgentInfo agentInfo : agentInfos) {
            agentInfoDao.insert(agentInfo);
            applicationIndexDao.insert(agentInfo);
        }

        //4,topology data
        byte[] traceId = converter.getTraceId();
        TransactionId id = TransactionIdUtils.parseTransactionId(traceId);
        TransactionEventKey key = new TransactionEventKey(id.getAgentId(), id.getAgentStartTime(), id.getTransactionSequence());
        TransactionEventValue value = new TransactionEventValue(key, id.getAgentId());

        topoService.updateTopo(value, converter.getStartTime());

    }
}
