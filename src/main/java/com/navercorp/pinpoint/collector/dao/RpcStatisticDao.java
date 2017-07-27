package com.navercorp.pinpoint.collector.dao;

import com.navercorp.pinpoint.thrift.dto.TSpan;

/**
 * Created by aires on 2016/11/22.
 */
public interface RpcStatisticDao {
    void update(TSpan span);
}
