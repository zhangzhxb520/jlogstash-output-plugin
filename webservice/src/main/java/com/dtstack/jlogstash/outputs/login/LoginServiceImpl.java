package com.dtstack.jlogstash.outputs.login;

import com.dtstack.jlogstash.outputs.exception.LoginException;
import com.dtstack.jlogstash.outputs.util.WebServiceClientUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.rpc.ServiceException;
import java.rmi.RemoteException;
import java.util.LinkedHashMap;

/**
 * 登陆服务
 *
 * @author zxb
 * @version 1.0.0
 *          2017年05月09日 17:09
 * @since 1.0.0
 */
public class LoginServiceImpl implements LoginService {

    private static final String LOGIN_NAME = "loginName";
    private static final String PASSWORD = "password";
    private static final Logger LOGGER = LoggerFactory.getLogger(LoginServiceImpl.class);
    private static final long MINUTE = 1000 * 60;
    private static volatile String sessionId;
    private String loginName;
    private String password;
    private String url;
    private String targetNameSpace;
    private String operationName = "login";
    private volatile Long lastRefreshTime;

    public LoginServiceImpl(String loginName, String password, String url, String targetNameSpace) {
        this.loginName = loginName;
        this.password = password;
        this.url = url;
        this.targetNameSpace = targetNameSpace;
    }

    @Override
    public String login() {
        // 设置sessionId
        if (sessionId == null) {
            synchronized (LoginServiceImpl.class) {
                if (sessionId == null) {
                    sessionId = getSessionId();
                }
            }
        }
        return sessionId;
    }

    @Override
    public void refresh() {
        synchronized (LoginServiceImpl.class) {
            if (needRefresh()) {
                lastRefreshTime = System.nanoTime();
                sessionId = getSessionId();
            }
        }
    }

    private boolean needRefresh() {
        if (lastRefreshTime == null) {
            return true;
        }
        if (System.nanoTime() - lastRefreshTime > MINUTE) {
            return true;
        }
        return false;
    }

    private String getSessionId() {
        LinkedHashMap<String, Object> parameters = new LinkedHashMap<String, Object>();
        parameters.put(LOGIN_NAME, loginName);
        parameters.put(PASSWORD, password);

        try {
            String sessionId = WebServiceClientUtil.invoke(url, targetNameSpace, operationName, parameters);
            LOGGER.info("result：{}", sessionId);
            if (sessionId != null) {
                if (!sessionId.startsWith("!")) {
                    return sessionId;
                }
            }
        } catch (ServiceException e) {
            LOGGER.error("service connect error, url:" + url, e);
        } catch (RemoteException e) {
            LOGGER.error("invoke service error, url:" + url + ", operation:" + operationName, e);
        }
        throw new LoginException("login failed");
    }

    public void setLoginName(String loginName) {
        this.loginName = loginName;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setTargetNameSpace(String targetNameSpace) {
        this.targetNameSpace = targetNameSpace;
    }

    public void setOperationName(String operationName) {
        this.operationName = operationName;
    }
}
