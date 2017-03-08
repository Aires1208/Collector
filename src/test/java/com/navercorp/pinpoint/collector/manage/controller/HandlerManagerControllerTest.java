package com.navercorp.pinpoint.collector.manage.controller;

import com.navercorp.pinpoint.collector.manage.HandlerManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.model;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Created by root on 17-1-6.
 */
@RunWith(MockitoJUnitRunner.class)
public class HandlerManagerControllerTest {

    private MockMvc mockMvc;

    @Mock
    private HandlerManager handlerManager;

    @InjectMocks
    private HandlerManagerController controller = new HandlerManagerController();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        this.mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    public void enableAccess_should_return_success() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/admin/enableAccess")
                .contentType(MediaType.APPLICATION_JSON)
                .content("true")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(model().size(1))
                .andExpect(model().attribute("result", "success"));
    }

    @Test
    public void enableAccess_should_return_fail() throws Exception {
        //given
        final String message = "test error message";

        //when
        Mockito.doThrow(new RuntimeException(message)).when(handlerManager).enableAccess();

        //then
        mockMvc.perform(MockMvcRequestBuilders.get("/admin/enableAccess")
                .content("true")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(model().size(2))
                .andExpect(model().attribute("result", "fail"))
                .andExpect(model().attribute("message", message));
    }

    @Test
    public void disableAccess_should_return_success() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/admin/disableAccess")
                .contentType(MediaType.APPLICATION_JSON)
                .content("true")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(model().size(1))
                .andExpect(model().attribute("result", "success"));
    }

    @Test
    public void disableAccess_should_return_fail() throws Exception {
        //given
        final String message = "test error message";

        //when
        Mockito.doThrow(new RuntimeException(message)).when(handlerManager).disableAccess();

        //then
        mockMvc.perform(MockMvcRequestBuilders.get("/admin/disableAccess")
                .contentType(MediaType.APPLICATION_JSON)
                .content("true")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(model().size(2))
                .andExpect(model().attribute("result", "fail"))
                .andExpect(model().attribute("message", message));
    }

    @Test
    public void should_return_admin_disable() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/admin/isEnable")
                .contentType(MediaType.APPLICATION_JSON)
                .content("true")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(model().size(2))
                .andExpect(model().attribute("result", "success"))
                .andExpect(model().attribute("isEnable", false));
    }

    @Test
    public void should_return_admin_enable() throws Exception {
        Mockito.when(handlerManager.isEnable()).thenReturn(true);

        mockMvc.perform(MockMvcRequestBuilders.get("/admin/isEnable")
                .contentType(MediaType.APPLICATION_JSON)
                .content("true")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(model().size(2))
                .andExpect(model().attribute("result", "success"))
                .andExpect(model().attribute("isEnable", true));
    }

}