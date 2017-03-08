package com.navercorp.pinpoint.collector.monitor;

import com.codahale.metrics.MetricRegistry;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.*;

/**
 * Created by root on 16-12-27.
 */
public class CollectorMetricTest {

    @Mock
    private MetricRegistry metricRegistry;

    @InjectMocks
    private CollectorMetric metric = new CollectorMetric();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void isEnable() throws Exception {
        metric.start();
        assertEquals(true, metric.isEnable());
    }

}