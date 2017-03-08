package com.navercorp.pinpoint.collector.util;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

import static org.junit.Assert.assertEquals;

/**
 * Created by root on 16-12-23.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(CollectorUtils.class)
public class CollectorUtilsTest {

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(ManagementFactory.class);
    }

    @Test
    public void getServerIdentifier() throws Exception {
        //
        RuntimeMXBean mxBean = PowerMockito.mock(RuntimeMXBean.class);

        PowerMockito.when(mxBean.getName()).thenReturn("test");
        PowerMockito.when(ManagementFactory.getRuntimeMXBean()).thenReturn(mxBean);

        String mxBeanName = CollectorUtils.getServerIdentifier();

        assertEquals(mxBeanName, "test");
    }

}