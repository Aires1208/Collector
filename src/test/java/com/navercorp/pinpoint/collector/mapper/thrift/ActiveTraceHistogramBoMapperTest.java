package com.navercorp.pinpoint.collector.mapper.thrift;

import com.navercorp.pinpoint.common.bo.ActiveTraceHistogramBo;
import com.navercorp.pinpoint.thrift.dto.TActiveTraceHistogram;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;

/**
 * Created by root on 16-12-21.
 */
public class ActiveTraceHistogramBoMapperTest {
    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void map() throws Exception {
        //given
        TActiveTraceHistogram histogram = new TActiveTraceHistogram();
        histogram.setActiveTraceCount(newArrayList(1, 2, 1, 3));
        histogram.setVersion((short) 0);
        histogram.setHistogramSchemaType(0);

        //when
        ActiveTraceHistogramBoMapper mapper = new ActiveTraceHistogramBoMapper();
        ActiveTraceHistogramBo activeTraceHistogramBo = mapper.map(histogram);

        //when
        ActiveTraceHistogramBo bo = new ActiveTraceHistogramBo(0, 0, newArrayList(1, 2, 1, 3));
        assertEquals(bo, activeTraceHistogramBo);
    }

}