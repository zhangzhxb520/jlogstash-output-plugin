package com.dtstack.jlogstash.outputs.util;

import org.junit.Test;

import java.util.LinkedHashMap;

import static org.junit.Assert.assertNotNull;

/**
 * @author zxb
 * @version 1.0.0
 *          2017年05月10日 14:12
 * @since 1.0.0
 */
public class WebServiceClientUtilTest {

    @Test
    public void invoke() throws Exception {
        String url = "http://localhost:8080/eview/services/GabTerminalDataServer?wsdl";
        String targetNameSpace = "http://localhost:8080/eview/services/GabTerminalDataServer";
        String operationName = "ServiceTest";
        LinkedHashMap<String, Object> params = new LinkedHashMap<String, Object>();
        params.put("SenderID", "xx");
        params.put("ServiceID", "xx");
        params.put("TestCode", "xx");
        params.put("TestParameter", "xx");

        String result = WebServiceClientUtil.invoke(url, targetNameSpace, operationName, params);
        assertNotNull(result);
        System.out.println(result);
    }

}