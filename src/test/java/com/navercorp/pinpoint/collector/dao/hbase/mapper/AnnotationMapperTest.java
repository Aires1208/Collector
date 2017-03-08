package com.navercorp.pinpoint.collector.dao.hbase.mapper;

import com.navercorp.pinpoint.common.bo.AnnotationBo;
import com.navercorp.pinpoint.common.bo.AnnotationBoList;
import com.navercorp.pinpoint.common.buffer.AutomaticBuffer;
import com.navercorp.pinpoint.common.buffer.Buffer;
import com.navercorp.pinpoint.common.hbase.HBaseTables;
import com.navercorp.pinpoint.common.trace.AnnotationKey;
import com.navercorp.pinpoint.thrift.dto.TAnnotation;
import com.navercorp.pinpoint.thrift.dto.TAnnotationValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;

/**
 * Created by root on 16-12-28.
 */
public class AnnotationMapperTest {
    @Test
    public void mapRow() throws Exception {
        //given
        final Result result = getResult();

        //when
        AnnotationMapper mapper = new AnnotationMapper();
        Map<Long, List<AnnotationBo>> annotationMap = mapper.mapRow(result, 0);

        //then
        assertEquals(1, annotationMap.size());
    }

    private Result getResult() {
        byte[] qualifier1 = Bytes.toBytes(10000L);

        TAnnotation annotation1 = new TAnnotation(AnnotationKey.API.getCode());
        TAnnotationValue value1 = new TAnnotationValue();
        value1.setStringValue("doPost()");
        annotation1.setValue(value1);

        TAnnotation annotation2 = new TAnnotation(AnnotationKey.SQL.getCode());
        TAnnotationValue value2 = new TAnnotationValue();
        value2.setStringValue("select * from history;");
        annotation2.setValue(value1);

        byte[] value = writeAnnotation(newArrayList(annotation1, annotation2));

        Cell cell1 = createCell(HBaseTables.TRACES_CF_ANNOTATION, qualifier1, value);

        return Result.create(newArrayList(cell1));
    }

    private Cell createCell(byte[] family, byte[] qualifier, byte[] value) {
        return CellUtil.createCell(HConstants.EMPTY_BYTE_ARRAY, family, qualifier, HConstants.LATEST_TIMESTAMP, KeyValue.Type.Maximum.getCode(), value);
    }

    private byte[] writeAnnotation(List<TAnnotation> annotations) {
        List<AnnotationBo> boList = new ArrayList<>(annotations.size());
        for (TAnnotation ano : annotations) {
            AnnotationBo annotationBo = new AnnotationBo(ano);
            boList.add(annotationBo);
        }

        Buffer buffer = new AutomaticBuffer(64);
        AnnotationBoList annotationBoList = new AnnotationBoList(boList);
        annotationBoList.writeValue(buffer);
        return buffer.getBuffer();
    }

}