package com.dtstack.jlogstash.outputs.util;

import org.apache.axis.client.Call;
import org.apache.axis.client.Service;
import org.apache.axis.encoding.XMLType;
import org.apache.axis.handlers.LogHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.namespace.QName;
import javax.xml.rpc.ParameterMode;
import javax.xml.rpc.ServiceException;
import java.rmi.RemoteException;
import java.util.Map;

/**
 * WebService调用工具类
 *
 * @author zxb
 * @version 1.0.0
 *          2017年05月09日 16:22
 * @since 1.0.0
 */
public class WebServiceClientUtil {

    private static final Integer TIMEOUT = 300000;

    private static LogHandler logHandler = new LogHandler();

    private static Logger logger = LoggerFactory.getLogger(WebServiceClientUtil.class);


    /**
     * WebService调用
     *
     * @param url             webservice地址
     * @param targetNameSpace 命名空间
     * @param operationName   将调用的方法名称
     * @param params          参数列表
     * @return 返回结果，String类型
     * @throws ServiceException 服务无法连接时抛出
     * @throws RemoteException  服务调用出错时抛出
     */
    public static String invoke(String url, String targetNameSpace, String operationName, Map<String, Object> params) throws ServiceException, RemoteException {
        Service service = new Service();
        Call call = (Call) service.createCall();

        call.setTimeout(TIMEOUT);
        call.setTargetEndpointAddress(url);
        call.setOperationName(new QName(targetNameSpace, operationName));
        call.setReturnType(XMLType.XSD_STRING);

        if (params != null) {
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                Class clazz = entry.getValue().getClass();
                QName xmlType = call.getTypeMapping().getTypeQName(clazz);
                call.addParameter(entry.getKey(), xmlType, clazz, ParameterMode.IN);
            }
        }
        call.setClientHandlers(logHandler, logHandler);
        Object[] args = params.values().toArray();
        return (String)call.invoke(args);
    }
}
