package com.navercorp.pinpoint.collector.dao;

import com.navercorp.pinpoint.collector.eventbus.TransactionEventKey;
import com.navercorp.pinpoint.common.bo.SpanBo;

import java.util.List;

public interface TransactionsDao {
    List<SpanBo> selectSpans(TransactionEventKey key);
}
