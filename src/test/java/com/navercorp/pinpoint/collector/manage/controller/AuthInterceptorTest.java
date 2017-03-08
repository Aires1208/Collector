package com.navercorp.pinpoint.collector.manage.controller;

import org.junit.Test;
import org.mockito.InjectMocks;
import org.omg.CORBA.Object;
import org.springframework.web.servlet.ModelAndViewDefiningException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by root on 16-12-28.
 */
public class AuthInterceptorTest {

    @InjectMocks
    private AuthInterceptor authInterceptor = new AuthInterceptor();

    @Test
    public void should_return_true_when_input_password() throws Exception {
        //given
        final String password = "Aa888888";
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        Object handler = mock(Object.class);

        //when
        when(request.getParameter(anyString())).thenReturn(password);
        setActiveTrue(authInterceptor);
        setPassword(password, authInterceptor);
        boolean result = authInterceptor.preHandle(request, response, handler);

        assertTrue(result);
    }

    @Test
    public void should_throw_out_exception_when_isActive_is_false() {
        //given
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        Object handler = mock(Object.class);

        //then
        try {
            authInterceptor.preHandle(request, response, handler);
        } catch (Exception e) {
            assertEquals("not activing rest api for admin.", ((ModelAndViewDefiningException) e).getModelAndView().getModel().get("message"));
        }
    }

    @Test
    public void should_throw_out_exception_when_password_invalid() throws NoSuchFieldException, IllegalAccessException {
        //given
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        Object handler = mock(Object.class);
        setActiveTrue(authInterceptor);
        setPassword("root123", authInterceptor);

        //then
        try {
            authInterceptor.preHandle(request, response, handler);
        } catch (Exception e) {
            assertEquals("not matched admin password.", ((ModelAndViewDefiningException) e).getModelAndView().getModel().get("message"));
        }
    }

    @Test
    public void should_throw_out_exception_when_input_invalid_password() {
        //given
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        Object handler = mock(Object.class);

        //then
        try {
            authInterceptor.preHandle(request, response, handler);
        } catch (Exception e) {
            assertEquals("not activing rest api for admin.", ((ModelAndViewDefiningException) e).getModelAndView().getModel().get("message"));
        }
    }

    private void setActiveTrue(AuthInterceptor authInterceptor) throws NoSuchFieldException, IllegalAccessException {
        Field isActive = authInterceptor.getClass().getDeclaredField("isActive");
        isActive.setAccessible(true);
        isActive.setBoolean(authInterceptor, true);
    }

    private void setPassword(String password, AuthInterceptor authInterceptor) throws NoSuchFieldException, IllegalAccessException {
        Field passwd = authInterceptor.getClass().getDeclaredField("password");
        passwd.setAccessible(true);
        passwd.set(authInterceptor, password);
    }
}