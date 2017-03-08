package com.navercorp.pinpoint.collector.dao.elasticsearch;

import com.navercorp.pinpoint.thrift.dto.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by root on 2/14/17.
 */
public class DataFactory {

    public static TProcesses getProcesses() {
        TProcesses tProcesses = new TProcesses();
        List<TProcessInfo> tProcessInfos = new ArrayList<TProcessInfo>();
        TProcessInfo tProcessInfo1 = new TProcessInfo();
        tProcessInfo1.setPID("1");
        tProcessInfo1.setProcess("java");
        tProcessInfo1.setCommand("/usr/bin/java");
        tProcessInfo1.setCpuTime(100);
        tProcessInfo1.setCpuUsage(0.09);
        tProcessInfo1.setVirt(3000);
        tProcessInfos.add(tProcessInfo1);

        TProcessInfo tProcessInfo2 = new TProcessInfo();
        tProcessInfo2.setPID("2");
        tProcessInfo2.setProcess("python");
        tProcessInfo2.setCommand("/usr/bin/python");
        tProcessInfo2.setCpuTime(1000);
        tProcessInfo2.setCpuUsage(0.89);
        tProcessInfo2.setVirt(4000);
        tProcessInfos.add(tProcessInfo2);

        tProcesses.setTProcessesVirt(tProcessInfos);
        tProcesses.setTProcessesCpuUsage(tProcessInfos);
        tProcesses.setTProcessesCpuTime(tProcessInfos);

        return tProcesses;
    }
    public static TNets getNets() {
        List<TNetInfoStatic> tNetInfoStatics = new ArrayList<TNetInfoStatic>();
        TNetInfoStatic tNetInfoStatic1 = new TNetInfoStatic();
        tNetInfoStatic1.setName("eth0");
        tNetInfoStatic1.setV4Address("272001");
        tNetInfoStatic1.setMacAddress("02234ajk");
        tNetInfoStatic1.setMtu(100);
        tNetInfoStatics.add(tNetInfoStatic1);

        TNetInfoStatic tNetInfoStatic2 = new TNetInfoStatic();
        tNetInfoStatic2.setName("lo");
        tNetInfoStatic2.setV4Address("2062011");
        tNetInfoStatic2.setMacAddress("12234ajk");
        tNetInfoStatic2.setMtu(1000);
        tNetInfoStatics.add(tNetInfoStatic2);



        List<TNetInfoDynamic> tNetInfoDynamics = new ArrayList<TNetInfoDynamic>();

        TNetInfoDynamic tNetInfoDynamic1 = new TNetInfoDynamic();
        tNetInfoDynamic1.setName("eth0");
        tNetInfoDynamic1.setReceiveBytes(10);
        tNetInfoDynamic1.setReceiveErrors(1);
        tNetInfoDynamic1.setTransmitBytes(100);
        tNetInfoDynamic1.setTransmitErrors(10);
        tNetInfoDynamic1.setColls(10);
        tNetInfoDynamics.add(tNetInfoDynamic1);

        TNetInfoDynamic tNetInfoDynamic2 = new TNetInfoDynamic();
        tNetInfoDynamic2.setName("lo");
        tNetInfoDynamic2.setReceiveBytes(100);
        tNetInfoDynamic2.setReceiveErrors(10);
        tNetInfoDynamic2.setTransmitBytes(1000);
        tNetInfoDynamic2.setTransmitErrors(100);
        tNetInfoDynamic2.setColls(100);
        tNetInfoDynamics.add(tNetInfoDynamic2);

        return new TNets(tNetInfoStatics,tNetInfoDynamics);
    }

    public static TCpus getCpus() {
        List<TCpuInfoStatic> tCpuInfoStatics = new ArrayList<TCpuInfoStatic>();
        TCpuInfoStatic tCpuInfoStatic1 = new TCpuInfoStatic();
        tCpuInfoStatic1.setProcessor("1");
        tCpuInfoStatic1.setVendor("vendor1");
        tCpuInfoStatic1.setModel("model1");
        tCpuInfoStatic1.setModelName("modelname1");
        tCpuInfoStatic1.setCpuFamily("family1");
        tCpuInfoStatic1.setCpuMHz("2.7");
        tCpuInfoStatic1.setCacheSize("200");
        tCpuInfoStatics.add(tCpuInfoStatic1);

        TCpuInfoStatic tCpuInfoStatic2 = new TCpuInfoStatic();
        tCpuInfoStatic2.setProcessor("2");
        tCpuInfoStatic2.setVendor("vendor2");
        tCpuInfoStatic2.setModel("model2");
        tCpuInfoStatic2.setModelName("modelname2");
        tCpuInfoStatic2.setCpuFamily("family2");
        tCpuInfoStatic2.setCpuMHz("3.7");
        tCpuInfoStatic2.setCacheSize("300");
        tCpuInfoStatics.add(tCpuInfoStatic2);

        List<TCpuInfoDynamic> tCpuInfoDynamics = new ArrayList<TCpuInfoDynamic>();

        TCpuInfoDynamic tCpuInfoDynamic1 = new TCpuInfoDynamic();
        tCpuInfoDynamic1.setProcessor("1");
        tCpuInfoDynamic1.setUser(10);
        tCpuInfoDynamic1.setNice(20);
        tCpuInfoDynamic1.setSystem(30);
        tCpuInfoDynamic1.setIdle(40);
        tCpuInfoDynamic1.setIowait(50);
        tCpuInfoDynamic1.setIrq(60);
        tCpuInfoDynamic1.setSoftirq(70);
        tCpuInfoDynamics.add(tCpuInfoDynamic1);

        TCpuInfoDynamic tCpuInfoDynamic2 = new TCpuInfoDynamic();
        tCpuInfoDynamic2.setProcessor("2");
        tCpuInfoDynamic2.setUser(100);
        tCpuInfoDynamic2.setNice(200);
        tCpuInfoDynamic2.setSystem(300);
        tCpuInfoDynamic2.setIdle(400);
        tCpuInfoDynamic2.setIowait(500);
        tCpuInfoDynamic2.setIrq(600);
        tCpuInfoDynamic2.setSoftirq(700);
        tCpuInfoDynamics.add(tCpuInfoDynamic2);



        return new TCpus(tCpuInfoStatics,tCpuInfoDynamics);
    }

    public static List<TDeviceInfo> getDevices() {
        List<TDeviceInfo> deviceInfos = new ArrayList<TDeviceInfo>();
        TDeviceInfo tDeviceInfo1 = new TDeviceInfo();
        tDeviceInfo1.setDeviceName("dm-0");
        tDeviceInfo1.setTps(10.0);
        tDeviceInfo1.setRead(100);
        tDeviceInfo1.setWrite(200);
        tDeviceInfo1.setReadPerSecond(11.0);
        tDeviceInfo1.setWritePerSecond(22.0);
        deviceInfos.add(tDeviceInfo1);

        TDeviceInfo tDeviceInfo2 = new TDeviceInfo();
        tDeviceInfo2.setDeviceName("dm-1");
        tDeviceInfo2.setTps(100.0);
        tDeviceInfo2.setRead(1000);
        tDeviceInfo2.setWrite(2000);
        tDeviceInfo2.setReadPerSecond(110.0);
        tDeviceInfo2.setWritePerSecond(220.0);
        deviceInfos.add(tDeviceInfo2);

        return deviceInfos;
    }

    public static TFileSystems getFileSystems() {
        List<TFileSystemInfo> tFileSystemInfos = new ArrayList<TFileSystemInfo>();
        TFileSystemInfo tFileSystemInfo1 = new TFileSystemInfo();
        tFileSystemInfo1.setFileSystem("/dev/sda1");
        tFileSystemInfo1.setMountedOn("/");
        tFileSystemInfo1.setTotal(100);
        tFileSystemInfo1.setFree(20);
        tFileSystemInfo1.setUsed(80);
        tFileSystemInfos.add(tFileSystemInfo1);

        TFileSystemInfo tFileSystemInfo2 = new TFileSystemInfo();
        tFileSystemInfo2.setFileSystem("/dev/sda2");
        tFileSystemInfo2.setMountedOn("/dev/shm");
        tFileSystemInfo2.setTotal(1000);
        tFileSystemInfo2.setFree(200);
        tFileSystemInfo2.setUsed(800);
        tFileSystemInfos.add(tFileSystemInfo2);

        return new TFileSystems(tFileSystemInfos);
    }

    public static TMemInfo getMemInfo() {
        TMemInfo memInfo = new TMemInfo();
        memInfo.setVmTotal(100);
        memInfo.setVmFree(30);
        memInfo.setVmUsed(70);

        return memInfo;
    }
}
