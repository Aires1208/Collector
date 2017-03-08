package com.navercorp.pinpoint.collector.mapper.thrift;

import com.navercorp.pinpoint.common.bo.JvmInfoBo;
import com.navercorp.pinpoint.thrift.dto.TJvmGcType;
import com.navercorp.pinpoint.thrift.dto.TJvmInfo;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by root on 16-12-21.
 */
public class JvmInfoBoMapperTest {
    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void map() throws Exception {
        //given
        TJvmInfo tJvmInfo = new TJvmInfo((short) 0);
        tJvmInfo.setVmVersion("1.8.0_111");
        tJvmInfo.setGcType(TJvmGcType.SERIAL);

        //when
        JvmInfoBoMapper mapper = new JvmInfoBoMapper();
        JvmInfoBo jvmInfoBo = mapper.map(tJvmInfo);

        //then
        assertEquals((short) 0, jvmInfoBo.getVersion());
        assertEquals("1.8.0_111", jvmInfoBo.getJvmVersion());
        assertEquals(TJvmGcType.SERIAL.name(), jvmInfoBo.getGcTypeName());
    }

}