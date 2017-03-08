package com.navercorp.pinpoint.collector.manage.controller;

import org.junit.Test;
import org.springframework.web.servlet.ModelAndView;

import static org.junit.Assert.*;

/**
 * Created by root on 16-12-23.
 */
public class ControllerUtilsTest {
    @Test
    public void createJsonView() throws Exception {
        //given
        String expect = "jsonView";

        //when
        final ModelAndView mv = ControllerUtils.createJsonView();

        //then
        assertEquals(mv.getViewName(), expect);
    }

    @Test
    public void createJsonView1() throws Exception {
        //
        //when
        final ModelAndView mvTrue = ControllerUtils.createJsonView(true);
        final ModelAndView mvFalse = ControllerUtils.createJsonView(false);

        //then
        assertEquals(mvTrue.getModel().get("result"), "success");
        assertEquals(mvFalse.getModel().get("result"), "fail");
    }

    @Test
    public void createJsonView2() throws Exception {
        //given
        Object message = "createJsonView";

        //then
        ModelAndView mvFalse = ControllerUtils.createJsonView(false, message);
        ModelAndView mvTrue = ControllerUtils.createJsonView(true, message);

        //then
        assertEquals(mvFalse.getModel().get("message"), message);
        assertEquals(mvTrue.getModel().get("message"), message);
        assertEquals(mvTrue.getModel().get("result"), "success");
        assertEquals(mvFalse.getModel().get("result"), "fail");
    }

}