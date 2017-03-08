package com.navercorp.pinpoint.collector.dao.hbase;

import com.google.common.base.Preconditions;
import com.navercorp.pinpoint.collector.dao.TransactionsDao;
import com.navercorp.pinpoint.collector.dao.hbase.mapper.XSpanMapper;
import com.navercorp.pinpoint.collector.eventbus.TransactionEventKey;
import com.navercorp.pinpoint.common.bo.SpanBo;
import com.navercorp.pinpoint.common.hbase.HBaseTables;
import com.navercorp.pinpoint.common.hbase.HbaseOperations2;
import com.sematext.hbase.wd.AbstractRowKeyDistributor;
import com.sematext.hbase.wd.RowKeyDistributorByHashPrefix;
import org.apache.hadoop.hbase.client.Get;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Repository
public class HbaseTransactionDao implements TransactionsDao {
    private AbstractRowKeyDistributor rowKeyDistributor = new RowKeyDistributorByHashPrefix(new RowKeyDistributorByHashPrefix.OneByteSimpleHash(64));

    @Autowired
    private HbaseOperations2 hbaseTemplate;

    @Autowired
    private XSpanMapper spanMapper;

    @Override
    public List<SpanBo> selectSpans(TransactionEventKey key) {
        Preconditions.checkArgument(key != null, "transactionKey(transactionId) must not be null.");

        List<byte[]> hBaseFamilyList = new ArrayList<>(2);
        hBaseFamilyList.add(HBaseTables.TRACES_CF_SPAN);
        hBaseFamilyList.add(HBaseTables.TRACES_CF_TERMINALSPAN);

        return getSpans(key, hBaseFamilyList);
    }

    private List<SpanBo> getSpans(TransactionEventKey key, List<byte[]> hBaseFamilyList) {
        Preconditions.checkArgument(hBaseFamilyList != null, "hBase Family List may not be null.");

        final Get get = new Get(rowKeyDistributor.getDistributedKey(key.getBytes()));

        return hbaseTemplate.get(HBaseTables.TRACES, get, spanMapper);
    }
}
