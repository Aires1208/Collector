package com.navercorp.pinpoint.collector.dao.elasticsearch;

import com.navercorp.pinpoint.thrift.dto.*;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by root on 2/13/17.
 */
public class ESJsonBuilder {


    public ESJsonBuilder() {

    }

    public String buildMemoryJson(TAgentStat tAgentStat) {
        String jsonData = null;
        try {
            TMemInfo tMemInfo = tAgentStat.getMemInfo();

            XContentBuilder jsonBuild = XContentFactory.jsonBuilder();
            jsonBuild.startObject()
                    .field(ESConst.AGENT_ID, tAgentStat.getAgentId())
                    .field(ESConst.AGENT_STARTTIME, tAgentStat.getStartTimestamp())
                    .field(ESConst.COLLECT_TIME, tAgentStat.getTimestamp())
                    .field(ESConst.VM_TOTAL, tMemInfo.getVmTotal())
                    .field(ESConst.VM_FREE, tMemInfo.getVmFree())
                    .field(ESConst.VM_USED, tMemInfo.getVmUsed())
                    .field(ESConst.PHY_TOTAL, tMemInfo.getMemTotal())
                    .field(ESConst.PHY_FREE, tMemInfo.getMemFree())
                    .field(ESConst.PHY_USED, tMemInfo.getMemUsed())
                    .field(ESConst.SWAP_TOTAL, tMemInfo.getSwapTotal())
                    .field(ESConst.SWAP_FREE, tMemInfo.getSwapFree())
                    .field(ESConst.SWAP_USED, tMemInfo.getSwapUsed())
                    .endObject();

            jsonData = jsonBuild.string();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return jsonData;
    }

    public List<String> buildFilesJson(TAgentStat tAgentStat) {
        List<String> jsonDatas = new ArrayList<>();
        try {
            TFileSystems tFileSystems = tAgentStat.getFileSystems();
            for (TFileSystemInfo tFileSystemInfo : tFileSystems.getTFileSystemInfos()) {
                XContentBuilder jsonBuild = XContentFactory.jsonBuilder();
                jsonBuild.startObject()
                        .field(ESConst.AGENT_ID, tAgentStat.getAgentId())
                        .field(ESConst.AGENT_STARTTIME, tAgentStat.getStartTimestamp())
                        .field(ESConst.COLLECT_TIME, tAgentStat.getTimestamp())
                        .field(ESConst.FILE_SYSTEM, tFileSystemInfo.getFileSystem())
                        .field(ESConst.FILE_MOUNTON, tFileSystemInfo.getMountedOn())
                        .field(ESConst.FILE_TOTAL, tFileSystemInfo.getTotal())
                        .field(ESConst.FILE_FREE, tFileSystemInfo.getFree())
                        .field(ESConst.FILE_USED, tFileSystemInfo.getUsed())
                        .endObject();

                jsonDatas.add(jsonBuild.string());
            }



        } catch (IOException e) {
            e.printStackTrace();
        }

        return jsonDatas;
    }

    public List<String> buildDevicesJson(TAgentStat tAgentStat) {
        List<String> jsonDatas = new ArrayList<String>();
        try {
            TDevices tDevices = tAgentStat.getDevices();

            for(TDeviceInfo tDeviceInfo : tDevices.getTDevices()) {
                XContentBuilder jsonBuild = XContentFactory.jsonBuilder();
                jsonBuild.startObject()
                        .field(ESConst.AGENT_ID, tAgentStat.getAgentId())
                        .field(ESConst.AGENT_STARTTIME, tAgentStat.getStartTimestamp())
                        .field(ESConst.COLLECT_TIME, tAgentStat.getTimestamp())
                        .field(ESConst.DEVICE_NAME, tDeviceInfo.getDeviceName())
                        .field(ESConst.DEVICE_TPS, tDeviceInfo.getTps())
                        .field(ESConst.DEVICE_READ, tDeviceInfo.getRead())
                        .field(ESConst.DEVICE_WRITE, tDeviceInfo.getWrite())
                        .field(ESConst.DEVICE_READ_PERSECOND, tDeviceInfo.getReadPerSecond())
                        .field(ESConst.DEVICE_WRITE_PERSECOND, tDeviceInfo.getWritePerSecond())
                        .endObject();

                jsonDatas.add(jsonBuild.string());
            }



        } catch (IOException e) {
            e.printStackTrace();
        }

        return jsonDatas;
    }

    public List<String> buildCpusJson(TAgentStat tAgentStat) {
        List<String> jsonDatas = new ArrayList<String>();
        try {
            TCpus tCpus = tAgentStat.getCpus();
            List<TCpuInfoStatic> tCpuInfoStatics = tCpus.getTCpuInfoStatics();
            List<TCpuInfoDynamic> tCpuInfoDynamics = tCpus.getTCpuInfoDynamics();
            Map<String,TCpuInfoDynamic> tCpuInfoDynamicHashMap = new HashMap<>();

            for(TCpuInfoDynamic tCpuInfoDynamic : tCpuInfoDynamics) {
                tCpuInfoDynamicHashMap.put(tCpuInfoDynamic.getProcessor(),tCpuInfoDynamic);
            }

            for(TCpuInfoStatic tCpuInfoStatic : tCpuInfoStatics) {
                TCpuInfoDynamic tCpuInfoDynamic = tCpuInfoDynamicHashMap.get(tCpuInfoStatic.getProcessor());
                if(tCpuInfoDynamic == null) {
                    tCpuInfoDynamic = new TCpuInfoDynamic();
                }
                XContentBuilder jsonBuild = XContentFactory.jsonBuilder();
                jsonBuild.startObject()
                        .field(ESConst.AGENT_ID, tAgentStat.getAgentId())
                        .field(ESConst.AGENT_STARTTIME, tAgentStat.getStartTimestamp())
                        .field(ESConst.COLLECT_TIME, tAgentStat.getTimestamp())
                        .field(ESConst.CPU_ID, tCpuInfoStatic.getProcessor())
                        .field(ESConst.CPU_VENDOR, tCpuInfoStatic.getVendor())
                        .field(ESConst.CPU_FAMILY, tCpuInfoStatic.getCpuFamily())
                        .field(ESConst.CPU_MODEL, tCpuInfoStatic.getModel())
                        .field(ESConst.CPU_MODEL_NAME, tCpuInfoStatic.getModelName())
                        .field(ESConst.CPU_MHZ, tCpuInfoStatic.getCpuMHz())
                        .field(ESConst.CPU_CACHE, tCpuInfoStatic.getCacheSize())

                        .field(ESConst.CPU_USER, tCpuInfoDynamic.getUser())
                        .field(ESConst.CPU_NICE, tCpuInfoDynamic.getNice())
                        .field(ESConst.CPU_SYSTEM, tCpuInfoDynamic.getSystem())
                        .field(ESConst.CPU_IDEL, tCpuInfoDynamic.getIdle())
                        .field(ESConst.CPU_IOWAIT, tCpuInfoDynamic.getIowait())
                        .field(ESConst.CPU_IRQ, tCpuInfoDynamic.getIrq())
                        .field(ESConst.CPU_SOFTIRQ, tCpuInfoDynamic.getSoftirq())

                        .endObject();

                jsonDatas.add(jsonBuild.string());
            }



        } catch (IOException e) {
            e.printStackTrace();
        }

        return jsonDatas;
    }

    public List<String> buildNetsJson(TAgentStat tAgentStat) {
        List<String> jsonDatas = new ArrayList<String>();
        try {
            TNets tNets = tAgentStat.getNets();
            List<TNetInfoStatic> tNetInfoStatics = tNets.getTNetInfoStatics();
            List<TNetInfoDynamic> tNetInfoDynamics = tNets.getTNetInfoDynamics();
            Map<String,TNetInfoDynamic> tCpuInfoDynamicHashMap = new HashMap<>();

            for(TNetInfoDynamic tNetInfoDynamic : tNetInfoDynamics) {
                tCpuInfoDynamicHashMap.put(tNetInfoDynamic.getName(),tNetInfoDynamic);
            }

            for(TNetInfoStatic tNetInfoStatic : tNetInfoStatics) {
                TNetInfoDynamic tNetInfoDynamic = tCpuInfoDynamicHashMap.get(tNetInfoStatic.getName());
                if(tNetInfoDynamic == null) {
                    tNetInfoDynamic = new TNetInfoDynamic();
                }
                XContentBuilder jsonBuild = XContentFactory.jsonBuilder();
                jsonBuild.startObject()
                        .field(ESConst.AGENT_ID, tAgentStat.getAgentId())
                        .field(ESConst.AGENT_STARTTIME, tAgentStat.getStartTimestamp())
                        .field(ESConst.COLLECT_TIME, tAgentStat.getTimestamp())
                        .field(ESConst.NET_NAME, tNetInfoStatic.getName())
                        .field(ESConst.NET_V4_ADDRESS, tNetInfoStatic.getV4Address())
                        .field(ESConst.NET_MAC_ADDRESS, tNetInfoStatic.getMacAddress())
                        .field(ESConst.NET_MTU, tNetInfoStatic.getMtu())

                        .field(ESConst.NET_RECEIVE_BYTES, tNetInfoDynamic.getReceiveBytes())
                        .field(ESConst.NET_RECEIVE_ERRORS, tNetInfoDynamic.getReceiveErrors())
                        .field(ESConst.NET_TRANSMIT_BYTES, tNetInfoDynamic.getTransmitBytes())
                        .field(ESConst.NET_TRANSMIT_ERRORS, tNetInfoDynamic.getTransmitErrors())
                        .field(ESConst.NET_COLLS, tNetInfoDynamic.getColls())

                        .endObject();

                jsonDatas.add(jsonBuild.string());
            }



        } catch (IOException e) {
            e.printStackTrace();
        }

        return jsonDatas;
    }

    public List<String> buildPrcessesJson(TAgentStat tAgentStat) {
        List<String> jsonDatas = new ArrayList<String>();
        try {
            TProcesses tProcesses = tAgentStat.getProcesses();
            Set<TProcessInfo> tProcessInfos = new HashSet<>();
            tProcessInfos.addAll(tProcesses.getTProcessesCpuUsage());
            tProcessInfos.addAll(tProcesses.getTProcessesCpuTime());
            tProcessInfos.addAll(tProcesses.getTProcessesVirt());

            for(TProcessInfo tProcessInfo : tProcessInfos) {
                XContentBuilder jsonBuild = XContentFactory.jsonBuilder();
                jsonBuild.startObject()
                        .field(ESConst.AGENT_ID, tAgentStat.getAgentId())
                        .field(ESConst.AGENT_STARTTIME, tAgentStat.getStartTimestamp())
                        .field(ESConst.COLLECT_TIME, tAgentStat.getTimestamp())
                        .field(ESConst.PROCESS_PID, tProcessInfo.getPID())
                        .field(ESConst.PROCESS_NAME, tProcessInfo.getProcess())
                        .field(ESConst.PROCESS_COMMAND, tProcessInfo.getCommand())
                        .field(ESConst.PROCESS_CPU_USAGE, tProcessInfo.getCpuUsage())
                        .field(ESConst.PROCESS_CPU_TIME, tProcessInfo.getCpuTime())
                        .field(ESConst.PROCESS_VIRT, tProcessInfo.getVirt())

                        .endObject();

                jsonDatas.add(jsonBuild.string());
            }



        } catch (IOException e) {
            e.printStackTrace();
        }

        return jsonDatas;
    }
}
