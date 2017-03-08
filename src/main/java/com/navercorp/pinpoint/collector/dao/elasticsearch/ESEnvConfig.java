package com.navercorp.pinpoint.collector.dao.elasticsearch;

import org.elasticsearch.common.settings.Settings;

/**
 * Created by root on 2/13/17.
 */
public class ESEnvConfig {

    public static Settings Settings() {
        Settings  settings = Settings.settingsBuilder()
                .put("cluster.name", "xelk1")
                .put("network.host", "10.62.100.142")
                .put("node.name", "node-client").build();
        return settings;
    }
}
