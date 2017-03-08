package com.navercorp.pinpoint.collector.usercase;

/**
 * Created by root on 16-12-14.
 */
public class ServiceInfo {
    public static final String DEFAULT_SERVICE_NAME_SEPARATOR = "_";

    private String applicationName;
    private String serviceName;
    private String serviceType;

    public ServiceInfo(String applicationName, String serviceName, String serviceType) {
        this.applicationName = applicationName;
        this.serviceName = serviceName;
        this.serviceType = serviceType;
    }

    public String getServiceName() {
        return applicationName + DEFAULT_SERVICE_NAME_SEPARATOR + serviceName;
    }

    public String getServiceType() {
        return serviceType;
    }

}
