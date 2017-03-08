package com.navercorp.pinpoint.collector.util;

import com.navercorp.pinpoint.collector.usercase.ServiceInfo;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by root on 16-12-13.
 */
public class ZipkinConvertUtilsTest {
    @Test
    public void should_separate_into_3_item_when_input_testApo_service_PYTHON() throws Exception {
        //given
        String serviceName = "testApp_service_PYTHON";

        ServiceInfo services = ZipkinConvertUtils.parseServiceName(serviceName);

        assertEquals("testApp_service", services.getServiceName());
        assertEquals("python", services.getServiceType());
    }

    @Test
    public void shoul_separate_into_1_item_when_input_testService() throws Exception {
        //given
        String serviceName = "testService";

        ServiceInfo services = ZipkinConvertUtils.parseServiceName(serviceName);

        assertEquals("testService_testService", services.getServiceName());
        assertEquals("undefined", services.getServiceType());
    }

    @Test
    public void should_return_11000_when_input_go() throws Exception {
        //given
        final String serviceType = "go";

        //when
        short servieType = ZipkinConvertUtils.findServiceType(serviceType);

        //then
        assertEquals(11000, servieType);
    }

    @Test
    public void should_return_10000_when_input_python() throws Exception {
        //given
        final String serviceType = "python";

        //when
        short servieType = ZipkinConvertUtils.findServiceType(serviceType);

        //then
        assertEquals(10000, servieType);
    }

    @Test
    public void should_return_undefined_serviceType_when_input_undefined() throws Exception {
        //given
        final String serviceType = "undefined";

        //when
        short servieType = ZipkinConvertUtils.findServiceType(serviceType);

        //then
        assertEquals(-1, servieType);
    }

}