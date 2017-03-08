package com.navercorp.pinpoint.collector.dao.elasticsearch;

/**
 * Created by root on 2/14/17.
 */
public interface ESConst {
	String INDEX = "smartsight";
	String MEMORY_TYPE = "memory";
	String DEVICE_TYPE = "device";
	String CPU_TYPE = "cpu";
	String FILE_TYPE = "file";
	String NET_TYPE = "net";
	String PROCESS_TYPE = "process";

	String AGENT_ID = "agentId";

	String AGENT_STARTTIME = "startTime";
	String COLLECT_TIME = "collectTime";

	String VM_TOTAL = "vmTotal";
	String VM_FREE = "vmFree";
	String VM_USED = "vmUsed";

	String PHY_TOTAL = "phyTotal";
	String PHY_FREE = "phyFree";
	String PHY_USED = "phyUsed";

	String SWAP_TOTAL = "swapTotal";
	String SWAP_FREE = "swapFree";

	String SWAP_USED = "swapUsed";
	String DEVICE_NAME = "deviceName";
	String DEVICE_TPS = "tps";
	String DEVICE_READ = "read";
	String DEVICE_WRITE = "write";
	String DEVICE_READ_PERSECOND = "readPerSecond";
	String DEVICE_WRITE_PERSECOND = "writePerSecond";

	String CPU_ID = "id";
	String CPU_VENDOR = "vendor";
	String CPU_FAMILY = "family";
	String CPU_MODEL = "model";
	String CPU_MODEL_NAME = "modelName";
	String CPU_MHZ = "mhz";
	String CPU_CACHE = "cache";
	String CPU_USER = "user";
	String CPU_NICE = "nice";
	String CPU_SYSTEM= "system";
	String CPU_IDEL = "idle";
	String CPU_IOWAIT = "iowait";
	String CPU_IRQ = "irq";
	String CPU_SOFTIRQ = "softirq";

	String FILE_SYSTEM = "fileSystem";
	String FILE_MOUNTON = "mountedOn";
	String FILE_TOTAL = "total";
	String FILE_USED = "used";
	String FILE_FREE = "free";

	String NET_NAME = "name";
	String NET_V4_ADDRESS = "v4Address";
	String NET_MAC_ADDRESS = "macAddress";
	String NET_MTU = "mtu";
	String NET_RECEIVE_BYTES = "receiveBytes";
	String NET_RECEIVE_ERRORS = "receiveErrors";
	String NET_TRANSMIT_BYTES = "transmitBytes";
	String NET_TRANSMIT_ERRORS = "transmitErrors";
	String NET_COLLS = "colls";

	String PROCESS_PID = "pid";
	String PROCESS_NAME = "process";
	String PROCESS_COMMAND = "comand";
	String PROCESS_CPU_USAGE = "cpuUsage";
	String PROCESS_CPU_TIME = "cpuTime";
	String PROCESS_VIRT = "virt";
}
