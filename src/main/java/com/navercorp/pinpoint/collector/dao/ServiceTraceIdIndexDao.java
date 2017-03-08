package com.navercorp.pinpoint.collector.dao;


import com.navercorp.pinpoint.thrift.dto.TSpan;

/**
 * Created by root on 16-11-12.
 */
public interface ServiceTraceIdIndexDao {
    /**
     * @param span
     */
    void update(TSpan span);
}
