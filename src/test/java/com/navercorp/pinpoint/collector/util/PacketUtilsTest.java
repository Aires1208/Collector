package com.navercorp.pinpoint.collector.util;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.net.DatagramPacket;

import static org.junit.Assert.assertEquals;

/**
 * Created by root on 16-12-22.
 */
public class PacketUtilsTest {

    @Test
    public void dumpDatagramPacket() throws Exception {
        //given
        String data = "testdata";
        DatagramPacket datagramPacket = new DatagramPacket(Bytes.toBytes(data), 0, data.length());

        //when
        String packet = PacketUtils.dumpDatagramPacket(datagramPacket);
        String nullPacket = PacketUtils.dumpDatagramPacket(null);

        //then
        assertEquals(data, packet);
        assertEquals("null", nullPacket);
    }

    @Test
    public void dumpByteArray() throws Exception {
        //given
        String data = "testdata";
        byte[] binaryData = Bytes.toBytes(data);

        //when
        String packet = PacketUtils.dumpByteArray(binaryData);
        String nullPacket = PacketUtils.dumpByteArray(null);

        //then
        assertEquals(data, packet);
        assertEquals("null", nullPacket);

    }

}