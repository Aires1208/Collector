package com.navercorp.pinpoint.collector.dao;

import com.navercorp.pinpoint.thrift.dto.TSpan;

/**
 * Created by 10183966 on 2016/11/22.
 */
public interface RpcStatisticDao {
    void update(TSpan span);
}
