package com.navercorp.pinpoint.collector.dao.elasticsearch;

import org.elasticsearch.client.Client;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Created by root on 2/14/17.
 */
@Ignore
public class ESMaintainTest {




    @Test
    public void testDefineIndexTypeMapping_for_memory() throws Exception {

        ESMaintain esMaintain = new ESMaintain();
        Client client = esMaintain.getClient();


//        esMaintain.deleteType(ESConst.INDEX,ESConst.MEMORY_TYPE);

        esMaintain.defineMemoryIndexTypeMapping();

        client.close();
    }

    @Test
    public void testDefineIndexTypeMapping_for_Device() throws Exception {

        ESMaintain esMaintain = new ESMaintain();
        Client client = esMaintain.getClient();

//        esMaintain.deleteType(ESConst.INDEX,ESConst.DEVICE_TYPE);

        esMaintain.defineDeviceIndexTypeMapping();

        client.close();
    }

    @Test
    public void testDefineTypeMapping_for_CPU() throws Exception {

        ESMaintain esMaintain = new ESMaintain();
        Client client = esMaintain.getClient();

//        esMaintain.deleteType(ESConst.INDEX,ESConst.DEVICE_TYPE);

        esMaintain.defineCPUIndexTypeMapping();

        client.close();
    }

    @Ignore
    @Test
    public void testDefineTypeMapping_for_FILE() throws Exception {

        ESMaintain esMaintain = new ESMaintain();
        Client client = esMaintain.getClient();

//        esMaintain.deleteType(ESConst.INDEX,ESConst.DEVICE_TYPE);

        esMaintain.defineFileIndexTypeMapping();

        client.close();
    }

    @Test
    public void testDefineTypeMapping_for_NET() throws Exception {

        ESMaintain esMaintain = new ESMaintain();
        Client client = esMaintain.getClient();


        esMaintain.defineNetIndexTypeMapping();

        client.close();
    }

    @Test
    public void testDefineTypeMapping_for_PROCESS() throws Exception {

        ESMaintain esMaintain = new ESMaintain();
        Client client = esMaintain.getClient();

        esMaintain.deleteIndex(ESConst.INDEX);
        esMaintain.createIndex(ESConst.INDEX);
        esMaintain.defineProcessIndexTypeMapping();

        client.close();
    }

    @Test
    public void testDelete_type() {
        ESMaintain esMaintain = new ESMaintain();
        Client client = esMaintain.getClient();


        esMaintain.deleteIndex(ESConst.INDEX);

        client.close();
    }

      @Test
    public void testCreate_type() {
        ESMaintain esMaintain = new ESMaintain();
        Client client = esMaintain.getClient();

          esMaintain.deleteIndex(ESConst.INDEX);
        esMaintain.createIndex(ESConst.INDEX);

        client.close();
    }


}