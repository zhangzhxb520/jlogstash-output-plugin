package com.dtstack.jlogstash.outputs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.outputs.login.LoginService;
import com.dtstack.jlogstash.outputs.login.LoginServiceImpl;
import com.dtstack.jlogstash.outputs.util.WebServiceClientUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.rpc.ServiceException;
import java.rmi.RemoteException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Webservice
 *
 * @author zxb
 * @version 1.0.0
 *          2017年03月24日 10:34
 * @since Jdk1.6
 */
public class Webservice extends BaseOutput {

    private static final String SESSION_ID = "sessionId";
    private static final String FUNC_CODE = "funccode";
    private static final String ARGS = "args";
    private static final Logger LOGGER = LoggerFactory.getLogger(Webservice.class);
    @Required(required = true)
    private static String url;
    @Required(required = true)
    private static String targetNameSpace;
    @Required(required = true)
    private static String operationName;
    @Required(required = true)
    private static String funccode;
    @Required(required = true)
    private static String username;
    @Required(required = true)
    private static String password;
    private LoginService loginService = new LoginServiceImpl(username, password, url, targetNameSpace);

    public Webservice(Map config) {
        super(config);
    }

    public void prepare() {
    }

    protected void emit(Map event) {
        String json = JSON.toJSONString(event);
        String sessionId = loginService.login();
        Map<String, Object> parameters = new LinkedHashMap<String, Object>();
        parameters.put(SESSION_ID, sessionId);
        parameters.put(FUNC_CODE, funccode);
        parameters.put(ARGS, json);

        LOGGER.info("invoke parameter: {}", parameters);

        try {

            String result = WebServiceClientUtil.invoke(url, targetNameSpace, operationName, parameters);
            LOGGER.info("invoke result: {}", result);

            if (result != null) {
                JSONObject jsonObject = JSON.parseObject(result);
                String state = (String) jsonObject.get("state");
                if ("CRJ1002".equals(state)) {   //防止回话失效
                    loginService.refresh();
                }
            }
        } catch (ServiceException e) {
            LOGGER.error("service connect error", e);
        } catch (RemoteException e) {
            LOGGER.error("invoke remote service error", e);
        }
    }

    @Override
    public void release() {
    }
}
