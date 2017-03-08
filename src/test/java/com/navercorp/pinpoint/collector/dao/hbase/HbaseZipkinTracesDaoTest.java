package com.navercorp.pinpoint.collector.dao.hbase;

import com.google.common.collect.ImmutableList;
import com.navercorp.pinpoint.collector.dao.ZipkinTraceDao;
import com.navercorp.pinpoint.collector.dao.hbase.mapper.ZipkinTraceMapper;
import com.navercorp.pinpoint.collector.util.AcceptedTimeService;
import com.navercorp.pinpoint.common.hbase.HbaseOperations2;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import zipkin.Annotation;
import zipkin.BinaryAnnotation;
import zipkin.Endpoint;
import zipkin.Span;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class HbaseZipkinTracesDaoTest {

    @Mock
    private AcceptedTimeService acceptedTimeService;

    @Mock
    private HbaseOperations2 hbaseTemplate;

    @Mock
    private ZipkinTraceMapper zipkinTraceMapper;

    @InjectMocks
    private ZipkinTraceDao zipkinTraceDao = new HbaseZipkinTracesDao();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void insert() throws Exception {
        //given
        long timestampUs = System.nanoTime() / 1000;
        Span span = buildZipkinSpan(timestampUs);

        //when
        when(acceptedTimeService.getAcceptedTime()).thenReturn(timestampUs);

        zipkinTraceDao.insert(span);

        //then
        verify(hbaseTemplate).put(anyString(), any(Put.class));
    }

    @Test
    public void selectSpan() throws Exception {
        //given
        Long traceId = 1111L;
        long timestamp = System.nanoTime() / 1000;
        List<Span> zipkinspans = ImmutableList.of(buildZipkinSpan(timestamp));

        //when
        when(hbaseTemplate.get(anyString(), any(Get.class), any(ZipkinTraceMapper.class))).thenReturn(zipkinspans);
        List<Span> spanList = zipkinTraceDao.selectSpan(traceId);

        //then
        assertEquals(1, spanList.size());
    }

    private Span buildZipkinSpan(long timestampUs) {
        Span.Builder builder = Span.builder();
        builder.timestamp(timestampUs);
        builder.id(1000L);
        builder.traceId(1111L);
        builder.name("/getTimestamp");
        builder.duration(1000000L);

        List<Annotation> annotations = buildAnnotations(timestampUs);
        builder.annotations(annotations);

        List<BinaryAnnotation> binaryAnnotations = buildBinaryAnnotations();
        builder.binaryAnnotations(binaryAnnotations);

        return builder.build();
    }

    private List<BinaryAnnotation> buildBinaryAnnotations() {
        BinaryAnnotation.Builder builder = BinaryAnnotation.builder();
        builder.type(BinaryAnnotation.Type.STRING);
        builder.key("http.url");
        builder.value("http://10.62.100.241:8080/html/dashboard.html");
        builder.endpoint(buildEndPoint());

        return newArrayList(builder.build());
    }

    private List<Annotation> buildAnnotations(long timestampUs) {
        Annotation.Builder builder1 = Annotation.builder();
        builder1.value("SS");
        builder1.timestamp(timestampUs + 20000);
        builder1.endpoint(buildEndPoint());

        Annotation.Builder builder2 = Annotation.builder();
        builder2.value("SR");
        builder2.timestamp(timestampUs + 300000);
        builder2.endpoint(buildEndPoint());

        return newArrayList(builder1.build(), builder2.build());
    }

    private Endpoint buildEndPoint() {
        Endpoint.Builder builder = Endpoint.builder();

        builder.serviceName("zipkin_service1");
        builder.ipv4(2130706433);
        builder.port((short) 8088);
        return builder.build();
    }
}