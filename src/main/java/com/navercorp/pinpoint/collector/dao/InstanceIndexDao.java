package com.navercorp.pinpoint.collector.dao;

import com.navercorp.pinpoint.common.topo.domain.TopoLine;

public interface InstanceIndexDao {
    void update(String appName, long timestamp, TopoLine topoline);
}
