package com.navercorp.pinpoint.collector.dao;


import com.navercorp.pinpoint.common.topo.domain.TopoLine;

public interface ServiceIndexDao {
    void update(String appName, long timestamp, TopoLine traceTopo);
}
