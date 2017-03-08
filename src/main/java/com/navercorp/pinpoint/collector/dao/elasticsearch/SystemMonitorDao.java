package com.navercorp.pinpoint.collector.dao.elasticsearch;

import com.navercorp.pinpoint.thrift.dto.TAgentStat;
import com.navercorp.pinpoint.thrift.dto.TMemInfo;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 * Created by root on 2/12/17.
 */
public class SystemMonitorDao {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private Settings settings;

    public SystemMonitorDao(Settings settings) {

        this.settings = settings;

    }

    public void add(TAgentStat tAgentStat) {
        if (null != tAgentStat.getMemInfo()){
            addMemory(tAgentStat);
        }

        if (null != tAgentStat.getDevices()){
            addDevices(tAgentStat);
        }

        if (null != tAgentStat.getCpus()){
            addCpus(tAgentStat);
        }

        if (null != tAgentStat.getFileSystems()){
            addFileSystems(tAgentStat);
        }

        if (null != tAgentStat.getNets()){
            addNets(tAgentStat);
        }

        if (null != tAgentStat.getNets()){
            addProcess(tAgentStat);
        }
    }

    private void addProcess(TAgentStat tAgentStat) {
        Client client = null;
        try {
            client = getClient();

            List<String> jsonDatas = new ESJsonBuilder().buildPrcessesJson(tAgentStat);

            for(String jsonData : jsonDatas) {
                IndexResponse response = client.prepareIndex(ESConst.INDEX,ESConst.PROCESS_TYPE)
                        .setSource(jsonData).get();
                logger.info("insert processes data:{}", jsonData);
            }

        } finally {
            if(client != null) {
                client.close();
            }
        }
    }

    private void addNets(TAgentStat tAgentStat) {
        Client client = null;
        try {
            client = getClient();

            List<String> jsonDatas = new ESJsonBuilder().buildNetsJson(tAgentStat);

            for(String jsonData : jsonDatas) {
                IndexResponse response = client.prepareIndex(ESConst.INDEX,ESConst.NET_TYPE)
                        .setSource(jsonData).get();
                logger.info("insert nets data:{}", jsonData);
            }

        } finally {
            if(client != null) {
                client.close();
            }
        }
    }

    private void addFileSystems(TAgentStat tAgentStat) {
        Client client = null;
        try {
            client = getClient();

            List<String> jsonDatas = new ESJsonBuilder().buildFilesJson(tAgentStat);

            for(String jsonData : jsonDatas) {
                IndexResponse response = client.prepareIndex(ESConst.INDEX,ESConst.FILE_TYPE)
                        .setSource(jsonData).get();
                logger.info("insert files data:{}", jsonData);
            }

        } finally {
            if(client != null) {
                client.close();
            }
        }
    }

    private void addCpus(TAgentStat tAgentStat) {
        Client client = null;
        try {
            client = getClient();

            List<String> jsonDatas = new ESJsonBuilder().buildCpusJson(tAgentStat);

            for(String jsonData : jsonDatas) {
                IndexResponse response = client.prepareIndex(ESConst.INDEX,ESConst.CPU_TYPE)
                        .setSource(jsonData).get();
                logger.info("insert cpus data:{}", jsonData);
            }

        } finally {
            if(client != null) {
                client.close();
            }
        }
    }

    private Client getClient() {
        Client client = null;
        try {
            Settings settings = Settings.settingsBuilder()
                    .put("cluster.name", "xelk1")
                    .put("network.host", "10.62.100.142")
                    .put("client.transport.ping_timeout", "120s")
                    .put("node.name", "node-client").build();

            client = TransportClient.builder().settings(settings).build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.62.100.142"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return client;
    }

    private void addMemory(TAgentStat tAgentStat) {
        Client client = null;
        try {
            client = getClient();

            String jsonData = new ESJsonBuilder().buildMemoryJson(tAgentStat);
            IndexResponse response = client.prepareIndex(ESConst.INDEX,ESConst.MEMORY_TYPE)
                    .setSource(jsonData).get();


            logger.info("insert memory data:{}", jsonData);

        } finally {
            if(client != null) {
                client.close();
            }
        }
    }

    private void addDevices(TAgentStat tAgentStat) {
        Client client = null;
        try {
            client = getClient();

            List<String> jsonDatas = new ESJsonBuilder().buildDevicesJson(tAgentStat);

            for(String jsonData : jsonDatas) {
                IndexResponse response = client.prepareIndex(ESConst.INDEX,ESConst.DEVICE_TYPE)
                        .setSource(jsonData).get();
                logger.info("insert devices data:{}", jsonData);
            }

        } finally {
            if(client != null) {
                client.close();
            }
        }
    }

}
