package com.navercorp.pinpoint.collector.manage.controller;

import com.navercorp.pinpoint.collector.usercase.Converter;
import com.navercorp.pinpoint.thrift.dto.*;
import org.junit.Test;
import zipkin.Span;
import zipkin.internal.JsonCodec;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by root on 16-12-5.
 */
public class ConverterTest {
    @Test
    public void testJdbcSpans() throws Exception {
        List<Span> zipkinSpans = ZipkinSpanBuilder.read("jdbc_query.json");

        Converter converter = new Converter(zipkinSpans);

        List<TSpan> tSpans = converter.convert();

        assertEquals(1, tSpans.size());
        assertEquals(5, tSpans.get(0).getSpanEventListSize());
    }

    @Test
    public void should_return_3_pinpoint_span_when_given_python_tree_span() throws Exception {
        List<Span> zipkinSpans = ZipkinSpanBuilder.read("python_frontend_backendX2.json");

        Converter converter = new Converter(zipkinSpans);

        List<TSpan> tSpans = converter.convert();

        assertEquals(3, tSpans.size());

        TSpan svc1_span = getTSpan(tSpans, "http://localhost:5000/api");
        checkSvc1(svc1_span);
    }

    private void checkSvc1(TSpan span) {
        assertEquals(1483005720830L, span.getStartTime());
        assertEquals(4126, span.getElapsed());

        TSpanEvent event1 = span.getSpanEventList().get(0);
        assertEquals(5, event1.getStartElapsed());
        assertEquals(2107, event1.getEndElapsed());

        TSpanEvent event2 = span.getSpanEventList().get(1);
        assertEquals(2111, event2.getStartElapsed());
        assertEquals(2009, event2.getEndElapsed());
    }

    @Test
    public void testGet_ApiMataData() throws Exception {
        List<Span> zipkinSpans = ZipkinSpanBuilder.read("sleuth-spans.json");

        Converter converter = new Converter(zipkinSpans);

        List<TApiMetaData> apis = converter.buildApiInfo();

        List<TSqlMetaData> sqls = converter.buildSqlInfo();

        assertEquals(5, apis.size());
        assertEquals(1, sqls.size());
    }

    @Test
    public void should_2_pinpoint_span_when_2_node_2_zipkin_span() throws Exception {
        List<Span> zipkinSpans = ZipkinSpanBuilder.read("python-spans.json");

        Converter converter = new Converter(zipkinSpans);
        List<TSpan> pinpointSpans = converter.convert();

        assertEquals(2, pinpointSpans.size());
        TSpan service1 = pinpointSpans.get(1);
        checkService1span(service1);

        TSpan service2 = pinpointSpans.get(0);
        checkService2span(service2);

        List<TApiMetaData> apis = converter.buildApiInfo();
        checkAPIs(apis);

        List<TAgentInfo> agentInfos = converter.buildAgentInfos();
        checkAgentInfos(agentInfos);
    }

    private void checkAgentInfos(List<TAgentInfo> agentInfos) {
        assertEquals(2, agentInfos.size());
        assertEquals("app_service2_0", agentInfos.get(0).getAgentId());
        assertEquals("app_service2", agentInfos.get(0).getApplicationName());
    }

    private void checkAPIs(List<TApiMetaData> apis) {
        assertEquals(2, apis.size());
        assertEquals("app_service2_0", apis.get(0).getAgentId());
        assertEquals("http://localhost:9010/todo/api/v1.0/tasks", apis.get(0).getApiInfo());

        assertEquals("app_service1_0", apis.get(1).getAgentId());
        assertEquals("http://localhost:5000/api", apis.get(1).getApiInfo());

    }

    private void checkService2span(TSpan service2) {
        assertEquals(0x7576207829111908L, service2.getSpanId());
        assertEquals(1481728616027L, service2.getStartTime());
        assertEquals(2012, service2.getElapsed());
        assertEquals("http://localhost:9010/todo/api/v1.0/tasks", service2.getRpc());
        assertEquals(1, service2.getSpanEventListSize());
        assertEquals("app_service2", service2.getApplicationName());
        assertEquals("app_service2_0", service2.getAgentId());
        assertEquals((short) 10000, service2.getServiceType());

        TSpanEvent event = service2.getSpanEventList().get(0);
        assertEquals(0, event.getStartElapsed());
        assertEquals(2012, event.getEndElapsed());
        assertEquals(0, event.getSequence());
        assertEquals(1, event.getDepth());
        assertEquals(-1L, event.getNextSpanId());
    }

    private void checkService1span(TSpan service1) {
        assertEquals(1481728616025L, service1.getStartTime());
        assertEquals(0x360d83cc1bf13b49L, service1.getSpanId());
        assertEquals(2030, service1.getElapsed());
        assertEquals("http://localhost:5000/api", service1.getRpc());
        assertEquals(1, service1.getSpanEventListSize());

        TSpanEvent event = service1.getSpanEventList().get(0);
        assertEquals(0, event.getStartElapsed());
        assertEquals(0, event.getSequence());
        assertEquals(1, event.getDepth());
        assertEquals(2030, event.getEndElapsed());
        assertEquals(0x7576207829111908L, event.getNextSpanId());
    }

    @Test
    public void should_4_pinpointSpan_when_4_nodes_11_zipkinSpans() throws Exception {
        List<Span> zipkinSpans = ZipkinSpanBuilder.read("frontend-middleend-backendX2.json");

        Converter convertor = new Converter(zipkinSpans);
        List<TSpan> pinpointSpans = convertor.convert();

        assertEquals(4, pinpointSpans.size());

        TSpan frontSpan = getTSpan(pinpointSpans, "http:/");
        checkFrontSpan(frontSpan);

        TSpan middleSpan = getTSpan(pinpointSpans, "http:/middleend");
        checkMiddleSpan(middleSpan);

        TSpan back1Span = getTSpan(pinpointSpans, "http:/backend1");
        checkBack1Span(back1Span);

        TSpan back2Span = getTSpan(pinpointSpans, "http:/backend2");
        checkBack2Span(back2Span);
    }

    private TSpan getTSpan(List<TSpan> tSpen, String name) {
        for (TSpan tSpan : tSpen) {
            if (tSpan.getRpc().equals(name)) {
                return tSpan;
            }
        }
        return new TSpan();
    }

    private void checkBack2Span(TSpan back2Span) {
        assertEquals(0xe22804f46553d585L, back2Span.getSpanId());
        assertEquals(1480650317432L, back2Span.getStartTime());
        assertEquals(193, back2Span.getElapsed());
        assertEquals(2, back2Span.getSpanEventListSize());

        TSpanEvent spanEvent0 = back2Span.getSpanEventList().get(0);
        assertEquals(0, spanEvent0.getSequence());
        assertEquals(1, spanEvent0.getDepth());
        assertEquals(-1L, spanEvent0.getNextSpanId());
        assertEquals(6, spanEvent0.getStartElapsed());
        assertEquals(193, spanEvent0.getEndElapsed());

        TSpanEvent spanEvent1 = back2Span.getSpanEventList().get(1);
        assertEquals(1, spanEvent1.getSequence());
        assertEquals(2, spanEvent1.getDepth());
        assertEquals(47, spanEvent1.getStartElapsed());
    }

    private void checkBack1Span(TSpan back1Span) {
        assertEquals(-5243645538750269619L, back1Span.getSpanId());
        assertEquals(1480650316934L, back1Span.getStartTime());
        assertEquals(314, back1Span.getElapsed());
        assertEquals(2, back1Span.getSpanEventListSize());

        TSpanEvent spanEvent0 = back1Span.getSpanEventList().get(0);
        assertEquals(0, spanEvent0.getSequence());
        assertEquals(1, spanEvent0.getDepth());
        assertEquals(-1L, spanEvent0.getNextSpanId());
        assertEquals(3, spanEvent0.getStartElapsed());
        assertEquals(314, spanEvent0.getEndElapsed());

        TSpanEvent spanEvent1 = back1Span.getSpanEventList().get(1);
        assertEquals(1, spanEvent1.getSequence());
        assertEquals(2, spanEvent1.getDepth());
        assertEquals(47, spanEvent1.getStartElapsed());
    }

    private void checkMiddleSpan(TSpan middleSpan) {
        assertEquals(-7362231088530045481L, middleSpan.getSpanId());
        assertEquals(1480650316613L, middleSpan.getStartTime());
        assertEquals(981, middleSpan.getElapsed());
        assertEquals(4, middleSpan.getSpanEventListSize());

        TSpanEvent spanEvent0 = middleSpan.getSpanEventList().get(0);
        assertEquals(0, spanEvent0.getSequence());
        assertEquals(1, spanEvent0.getDepth());
        assertEquals(-1, spanEvent0.getNextSpanId());
        assertEquals(2, spanEvent0.getStartElapsed());
        assertEquals(981, spanEvent0.getEndElapsed());

        TSpanEvent spanEvent1 = middleSpan.getSpanEventList().get(1);
        assertEquals(1, spanEvent1.getSequence());
        assertEquals(2, spanEvent1.getDepth());
        assertEquals(-1, spanEvent1.getNextSpanId());
        assertEquals(28, spanEvent1.getStartElapsed());

        TSpanEvent spanEvent2 = middleSpan.getSpanEventList().get(2);
        assertEquals(2, spanEvent2.getSequence());
        assertEquals(3, spanEvent2.getDepth());
        if (spanEvent2.getRpc().equals("http:/backend1")) {
            assertEquals(-5243645538750269619L, spanEvent2.getNextSpanId());
            assertEquals(77, spanEvent2.getStartElapsed());
        } else {
            assertEquals(-2150463374350887547L, spanEvent2.getNextSpanId());
            assertEquals(616, spanEvent2.getStartElapsed());
        }

        TSpanEvent spanEvent3 = middleSpan.getSpanEventList().get(3);
        assertEquals(3, spanEvent3.getSequence());
        assertEquals(3, spanEvent3.getDepth());
        if (spanEvent3.getRpc().equals("http:/backend2")) {
            assertEquals(-2150463374350887547L, spanEvent3.getNextSpanId());
            assertEquals(616, spanEvent3.getStartElapsed());
        } else {
            assertEquals(-5243645538750269619L, spanEvent3.getNextSpanId());
            assertEquals(77, spanEvent3.getStartElapsed());
        }
    }

    private void checkFrontSpan(TSpan frontSpan) {
        assertEquals(0x8b86f55cef095dd9L, frontSpan.getSpanId());
        assertEquals(-1L, frontSpan.getParentSpanId());
        assertEquals(1480650312050L, frontSpan.getStartTime());
        assertEquals(6706, frontSpan.getElapsed());
        assertEquals(3, frontSpan.getSpanEventListSize());

        TSpanEvent spanEvent0 = frontSpan.getSpanEventList().get(0);
        assertEquals(0, spanEvent0.getSequence());
        assertEquals(1, spanEvent0.getDepth());
        assertEquals(-1L, spanEvent0.getNextSpanId());
        assertEquals(31, spanEvent0.getStartElapsed());
        assertEquals(6706, spanEvent0.getEndElapsed());

        TSpanEvent spanEvent1 = frontSpan.getSpanEventList().get(1);
        assertEquals(1, spanEvent1.getSequence());
        assertEquals(2, spanEvent1.getDepth());
        assertEquals(-1L, spanEvent1.getNextSpanId());
        assertEquals(1725, spanEvent1.getStartElapsed());

        TSpanEvent spanEvent2 = frontSpan.getSpanEventList().get(2);
        assertEquals(2, spanEvent2.getSequence());
        assertEquals(3, spanEvent2.getDepth());
        assertEquals(-7362231088530045481L, spanEvent2.getNextSpanId());
        assertEquals(3566, spanEvent2.getStartElapsed());
    }

    public static class ZipkinSpanBuilder {

        public static List<Span> read(String jsonPath) {
            URL resource = ConverterTest.class.getClassLoader().getResource(jsonPath);
            String content = read(resource);
            return JsonCodec.JSON.readSpans(content.getBytes());
        }

        public static String readJson(String jsonPath) {
            URL resource = ConverterTest.class.getClassLoader().getResource(jsonPath);
            return read(resource);
        }

        static String read(URL resource) {
            FileReader fileReader = null;
            BufferedReader bufferedReader = null;
            try {
                fileReader = new FileReader(new File(resource.getFile()));
                bufferedReader = new BufferedReader(fileReader);
                String content = "";
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    content += line;
                }
                return content;
            } catch (IOException e) {
                throw new IllegalStateException(e);
            } finally {
                if (fileReader != null) {
                    try {
                        fileReader.close();
                    } catch (IOException ignored) {
                    }
                }
                if (bufferedReader != null) {
                    try {
                        bufferedReader.close();
                    } catch (IOException ignored) {
                    }
                }
            }
        }
    }


}