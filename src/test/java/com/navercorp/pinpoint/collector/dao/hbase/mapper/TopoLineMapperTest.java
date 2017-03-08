package com.navercorp.pinpoint.collector.dao.hbase.mapper;

import com.navercorp.pinpoint.common.buffer.AutomaticBuffer;
import com.navercorp.pinpoint.common.buffer.Buffer;
import com.navercorp.pinpoint.common.topo.domain.XLink;
import com.navercorp.pinpoint.common.topo.domain.XNode;
import com.navercorp.pinpoint.common.util.BytesUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class TopoLineMapperTest {
    private static final byte[] SERVICETOPO_CF_N = BytesUtils.toBytes("N");
    private static final byte[] SERVICETOPO_CF_L = BytesUtils.toBytes("L");
    private static final String NODE_USER = "USER";
    private static final String NODE_CLIENT = "EMS_client";
    private static final String NODE_MAIN = "EMS_Main";
    private static final String NODE_ORACLE = "Minos_oracle";

    @Test
    public void should_return_expect_XNodeList_when_input_custom_nodecells() throws Exception {
        //given
        XNode xNode1 = new XNode(NODE_USER, (short) 2, 0, 0, 1);
        XNode xNode2 = new XNode(NODE_CLIENT, (short) 1000, 2000, 0, 1);
        XNode xNode3 = new XNode(NODE_MAIN, (short) 1000, 1000, 1, 1);
        XNode xNode4 = new XNode(NODE_ORACLE, (short) 2300, 0, 0, 1);
        final Result result = Result.create(newArrayList(createCell(SERVICETOPO_CF_N, BytesUtils.toBytes(xNode1.getName()), xNode1.writeValue()),
                createCell(SERVICETOPO_CF_N, BytesUtils.toBytes(xNode2.getName()), xNode2.writeValue()),
                createCell(SERVICETOPO_CF_N, BytesUtils.toBytes(xNode3.getName()), xNode3.writeValue()),
                createCell(SERVICETOPO_CF_N, BytesUtils.toBytes(xNode4.getName()), xNode4.writeValue())));
        List<XNode> expect_XNodes = newArrayList(xNode1, xNode2, xNode3, xNode4);

        //when
        XNodeMapper mapper = new XNodeMapper();
        List<XNode> destNodes = mapper.mapRow(result, 1);

        //then
        assertThat(destNodes, is(expect_XNodes));
    }

    @Test
    public void should_return_expect_XLinkList_when_input_custom_linkcells() throws Exception {
        //given
        XLink xLink1 = new XLink(NODE_USER, NODE_CLIENT, 0, 0, 1);
        XLink xLink2 = new XLink(NODE_CLIENT, NODE_MAIN, 300, 0, 1);
        XLink xLink3 = new XLink(NODE_MAIN, NODE_ORACLE, 0, 1, 1);

        final Result result = Result.create(newArrayList(createCell(SERVICETOPO_CF_L, createLinkQualifier(xLink1.getFrom(), xLink1.getTo()), xLink1.writeValue()),
                createCell(SERVICETOPO_CF_L, createLinkQualifier(xLink2.getFrom(), xLink2.getTo()), xLink2.writeValue()),
                createCell(SERVICETOPO_CF_L, createLinkQualifier(xLink3.getFrom(), xLink3.getTo()), xLink3.writeValue())));
        List<XLink> expect_Links = newArrayList(xLink1, xLink2, xLink3);

        //when
        XLinkMapper mapper = new XLinkMapper();
        List<XLink> destLinks = mapper.mapRow(result, 1);

        //then
        assertThat(destLinks, is(expect_Links));
    }


    private Cell createCell(byte[] family, byte[] qualifier, byte[] value) {
        return CellUtil.createCell(HConstants.EMPTY_BYTE_ARRAY, family, qualifier, HConstants.LATEST_TIMESTAMP, KeyValue.Type.Maximum.getCode(), value);
    }

    private byte[] createLinkQualifier(String from, String to) {
        final Buffer buffer = new AutomaticBuffer();
        buffer.putPrefixedString(from);
        buffer.putPrefixedString(to);
        return buffer.getBuffer();
    }
}