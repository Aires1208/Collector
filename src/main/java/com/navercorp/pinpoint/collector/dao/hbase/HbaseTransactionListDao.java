package com.navercorp.pinpoint.collector.dao.hbase;

import com.google.common.base.Preconditions;
import com.navercorp.pinpoint.collector.dao.TransactionListDao;
import com.navercorp.pinpoint.common.buffer.AutomaticBuffer;
import com.navercorp.pinpoint.common.buffer.Buffer;
import com.navercorp.pinpoint.common.hbase.HBaseTables;
import com.navercorp.pinpoint.common.hbase.HbaseOperations2;
import com.navercorp.pinpoint.thrift.dto.TSpan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

/**
 * Created by root on 16-10-17.
 */
@Repository
public class HbaseTransactionListDao implements TransactionListDao{
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private HbaseOperations2 hbaseTemplate;

    @Override
    public void insert(TSpan rootSpan) {
        Preconditions.checkArgument(rootSpan != null, new NullPointerException("Span data must not be empty."));

        String traceName = rootSpan.getRpc();
        if (rootSpan.getRpc() == null || rootSpan.getRpc().length() == 0) {
            traceName = "TransactionName Not Found";
        }

        final byte[] rowKey = Bytes.toBytes(rootSpan.getApplicationName());
        Put put = new Put(rowKey);

        final Buffer buffer = new AutomaticBuffer();
        buffer.put(rootSpan.getStartTime());
        buffer.putPrefixedString(rootSpan.getAgentId());

        put.addColumn(HBaseTables.TRANSACTIONLIST_CF_NAME, Bytes.toBytes(traceName), buffer.getBuffer());

        hbaseTemplate.put(HBaseTables.TRANSACTION_LIST, put);
        logger.info("HbaseTransactionListDao.update({})", rootSpan.getRpc());
    }
}
