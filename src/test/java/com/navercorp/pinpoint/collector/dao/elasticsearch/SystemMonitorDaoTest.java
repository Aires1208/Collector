package com.navercorp.pinpoint.collector.dao.elasticsearch;

import com.navercorp.pinpoint.thrift.dto.*;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by root on 2/13/17.
 */
public class SystemMonitorDaoTest {

    private Settings settings;

    @Before
    public void setUp() {
        settings = Settings.settingsBuilder()
                .put("cluster.name", "xelk1")
                .put("network.host", "10.62.100.142")
                .put("node.name", "node-client").build();
    }

    @Ignore
    @Test
    public void testAgentStat() throws Exception {
        //given
        String agentStartTime = "2017-02-13 14:50:00";
        System.out.println(getTime(agentStartTime));

        TAgentStat tAgentStat = new TAgentStat();
        tAgentStat.setAgentId("fm-agent80");
        tAgentStat.setStartTimestamp(getTime(agentStartTime));

        tAgentStat.setTimestamp(getTime("2017-02-14 14:51:00"));

        TMemInfo memInfo = DataFactory.getMemInfo();
        List<TDeviceInfo> tDeviceInfos = DataFactory.getDevices();

        tAgentStat.setMemInfo(memInfo);
        tAgentStat.setDevices(new TDevices(tDeviceInfos));

        TCpus tCpus = DataFactory.getCpus();
        tAgentStat.setCpus(tCpus);

        TFileSystems tFileSystems = DataFactory.getFileSystems();
        tAgentStat.setFileSystems(tFileSystems);

        TNets tNets = DataFactory.getNets();
        tAgentStat.setNets(tNets);

        TProcesses tProcesses = DataFactory.getProcesses();
        tAgentStat.setProcesses(tProcesses);


        //when
        SystemMonitorDao systemMonitorDao = new SystemMonitorDao(settings);
        systemMonitorDao.add(tAgentStat);

        //then
        System.out.println("the end");

    }


    private long getTime(String time) {
        long retTime = -1L;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        try {
            retTime = simpleDateFormat.parse(time).getTime();
        } catch (ParseException ex) {
            System.out.println(ex);
        }

        return retTime;
    }

}