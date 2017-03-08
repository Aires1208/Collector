package com.navercorp.pinpoint.collector.dao.elasticsearch;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by root on 2/14/17.
 */
public class ESMaintain {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private Client client = null;
    public ESMaintain() {
        client = getClient();
    }
    public Client getClient() {
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

    public void createIndex(String IndexName) {
        client.admin().indices().create(new CreateIndexRequest(IndexName))
                .actionGet();
    }

    public void deleteIndex(String IndexName) {
        IndicesExistsResponse indicesExistsResponse = client.admin().indices()
                .exists(new IndicesExistsRequest(new String[] { IndexName }))
                .actionGet();
        if (indicesExistsResponse.isExists()) {
            client.admin().indices().delete(new DeleteIndexRequest(IndexName))
                    .actionGet();
        }
    }
    
    public void deleteType(String IndexName, String TypeName){
        client.prepareDelete().setIndex(IndexName).setType(TypeName).execute().actionGet();
    }

    public void defineDeviceIndexTypeMapping() {
        try {
            XContentBuilder mapBuilder = XContentFactory.jsonBuilder();
            mapBuilder.startObject()
            .startObject(ESConst.DEVICE_TYPE)
                .startObject("properties")
                    .startObject(ESConst.AGENT_ID).field("type", "string").field("index", "not_analyzed").field("store", "yes").endObject()
                    .startObject(ESConst.AGENT_STARTTIME).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.COLLECT_TIME).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.DEVICE_NAME).field("type", "string").field("index", "not_analyzed").field("store", "yes").endObject()
                    .startObject(ESConst.DEVICE_READ).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.DEVICE_WRITE).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.DEVICE_TPS).field("type", "double").field("store", "yes").endObject()
                    .startObject(ESConst.DEVICE_READ_PERSECOND).field("type", "double").field("store", "yes").endObject()
                    .startObject(ESConst.DEVICE_WRITE_PERSECOND).field("type", "double").field("store", "yes").endObject()


                .endObject()
            .endObject()
            .endObject();

            PutMappingRequest putMappingRequest = Requests
                    .putMappingRequest(ESConst.INDEX).type(ESConst.DEVICE_TYPE)
                    .source(mapBuilder);
            client.admin().indices().putMapping(putMappingRequest).actionGet();
        } catch (IOException e) {
            logger.error(e.toString());
        }
    }

    public void defineMemoryIndexTypeMapping() {
        try {
            XContentBuilder mapBuilder = XContentFactory.jsonBuilder();
            mapBuilder.startObject()
             .startObject(ESConst.MEMORY_TYPE)
               .startObject("properties")
                    .startObject(ESConst.AGENT_ID).field("type", "string").field("index", "not_analyzed").field("store", "yes").endObject()
                    .startObject(ESConst.AGENT_STARTTIME).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.COLLECT_TIME).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.VM_TOTAL).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.VM_FREE).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.VM_USED).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.PHY_TOTAL).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.PHY_FREE).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.PHY_USED).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.SWAP_TOTAL).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.SWAP_FREE).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.SWAP_USED).field("type", "long").field("store", "yes").endObject()
               .endObject()
             .endObject()
             .endObject();

            PutMappingRequest putMappingRequest = Requests
                    .putMappingRequest(ESConst.INDEX).type(ESConst.MEMORY_TYPE)
                    .source(mapBuilder);
            client.admin().indices().putMapping(putMappingRequest).actionGet();
        } catch (IOException e) {
            logger.error(e.toString());
        }
    }

    public void defineCPUIndexTypeMapping() {
        try {
            XContentBuilder mapBuilder = XContentFactory.jsonBuilder();
            mapBuilder.startObject()
                    .startObject(ESConst.CPU_TYPE)
                    .startObject("properties")
                    .startObject(ESConst.AGENT_ID).field("type", "string").field("index", "not_analyzed").field("store", "yes").endObject()
                    .startObject(ESConst.AGENT_STARTTIME).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.COLLECT_TIME).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.CPU_ID).field("type", "String").field("store", "yes").endObject()
                    .startObject(ESConst.CPU_VENDOR).field("type", "String").field("store", "yes").endObject()
                    .startObject(ESConst.CPU_FAMILY).field("type", "String").field("store", "yes").endObject()
                    .startObject(ESConst.CPU_MODEL).field("type", "String").field("store", "yes").endObject()
                    .startObject(ESConst.CPU_MODEL_NAME).field("type", "String").field("store", "yes").endObject()
                    .startObject(ESConst.CPU_MHZ).field("type", "String").field("store", "yes").endObject()
                    .startObject(ESConst.CPU_CACHE).field("type", "String").field("store", "yes").endObject()
                    .startObject(ESConst.CPU_USER).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.CPU_NICE).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.CPU_SYSTEM).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.CPU_IDEL).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.CPU_IOWAIT).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.CPU_IRQ).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.CPU_SOFTIRQ).field("type", "long").field("store", "yes").endObject()
              .endObject()
              .endObject()
            .endObject();


            PutMappingRequest putMappingRequest = Requests
                    .putMappingRequest(ESConst.INDEX).type(ESConst.CPU_TYPE)
                    .source(mapBuilder);
            client.admin().indices().putMapping(putMappingRequest).actionGet();
        } catch (IOException e) {
            logger.error(e.toString());
        }
    }

    public void defineFileIndexTypeMapping() {
        try {
            XContentBuilder mapBuilder = XContentFactory.jsonBuilder();
            mapBuilder.startObject()
                    .startObject(ESConst.FILE_TYPE)
                    .startObject("properties")
                    .startObject(ESConst.AGENT_ID).field("type", "string").field("index", "not_analyzed").field("store", "yes").endObject()
                    .startObject(ESConst.AGENT_STARTTIME).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.COLLECT_TIME).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.FILE_SYSTEM).field("type", "String").field("index", "not_analyzed").field("store", "yes").endObject()
                    .startObject(ESConst.FILE_MOUNTON).field("type", "String").field("index", "not_analyzed").field("store", "yes").endObject()
                    .startObject(ESConst.FILE_TOTAL).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.FILE_FREE).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.FILE_USED).field("type", "long").field("store", "yes").endObject()
                    .endObject()
                    .endObject()
                    .endObject();


            PutMappingRequest putMappingRequest = Requests
                    .putMappingRequest(ESConst.INDEX).type(ESConst.FILE_TYPE)
                    .source(mapBuilder);
            client.admin().indices().putMapping(putMappingRequest).actionGet();
        } catch (IOException e) {
            logger.error(e.toString());
        }
    }

    public void defineNetIndexTypeMapping() {
        try {
            XContentBuilder mapBuilder = XContentFactory.jsonBuilder();
            mapBuilder.startObject()
                    .startObject(ESConst.NET_TYPE)
                    .startObject("properties")
                    .startObject(ESConst.AGENT_ID).field("type", "string").field("index", "not_analyzed").field("store", "yes").endObject()
                    .startObject(ESConst.AGENT_STARTTIME).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.COLLECT_TIME).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.NET_NAME).field("type", "String").field("index", "not_analyzed").field("store", "yes").endObject()
                    .startObject(ESConst.NET_V4_ADDRESS).field("type", "String").field("store", "yes").endObject()
                    .startObject(ESConst.NET_MAC_ADDRESS).field("type", "String").field("store", "yes").endObject()
                    .startObject(ESConst.NET_MTU).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.NET_RECEIVE_BYTES).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.NET_RECEIVE_ERRORS).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.NET_TRANSMIT_BYTES).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.NET_TRANSMIT_ERRORS).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.NET_COLLS).field("type", "long").field("store", "yes").endObject()

                    .endObject()
                    .endObject()
                    .endObject();


            PutMappingRequest putMappingRequest = Requests
                    .putMappingRequest(ESConst.INDEX).type(ESConst.NET_TYPE)
                    .source(mapBuilder);
            client.admin().indices().putMapping(putMappingRequest).actionGet();
        } catch (IOException e) {
            logger.error(e.toString());
        }
    }

    public void defineProcessIndexTypeMapping() {
        try {
            XContentBuilder mapBuilder = XContentFactory.jsonBuilder();
            mapBuilder.startObject()
                    .startObject(ESConst.PROCESS_TYPE)
                    .startObject("properties")
                    .startObject(ESConst.AGENT_ID).field("type", "string").field("index", "not_analyzed").field("store", "yes").endObject()
                    .startObject(ESConst.AGENT_STARTTIME).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.COLLECT_TIME).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.PROCESS_PID).field("type", "String").field("index", "not_analyzed").field("store", "yes").endObject()
                    .startObject(ESConst.PROCESS_NAME).field("type", "String").field("index", "not_analyzed").field("store", "yes").endObject()
                    .startObject(ESConst.PROCESS_COMMAND).field("type", "String").field("index", "not_analyzed").field("store", "yes").endObject()
                    .startObject(ESConst.PROCESS_VIRT).field("type", "long").field("store", "yes").endObject()
                    .startObject(ESConst.PROCESS_CPU_USAGE).field("type", "double").field("store", "yes").endObject()
                    .startObject(ESConst.PROCESS_CPU_TIME).field("type", "long").field("store", "yes").endObject()

                    .endObject()
                    .endObject()
                    .endObject();


            PutMappingRequest putMappingRequest = Requests
                    .putMappingRequest(ESConst.INDEX).type(ESConst.PROCESS_TYPE)
                    .source(mapBuilder);
            client.admin().indices().putMapping(putMappingRequest).actionGet();
        } catch (IOException e) {
            logger.error(e.toString());
        }
    }
}
