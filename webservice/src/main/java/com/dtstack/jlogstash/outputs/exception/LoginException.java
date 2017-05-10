package com.dtstack.jlogstash.outputs.exception;

/**
 * 登陆异常
 * @author zxb
 * @version 1.0.0
 *          2017年05月09日 17:17
 * @since 1.0.0
 */
public class LoginException extends RuntimeException {

    public LoginException() {
        super();
    }

    public LoginException(String message) {
        super(message);
    }

    public LoginException(String message, Throwable cause) {
        super(message, cause);
    }

    public LoginException(Throwable cause) {
        super(cause);
    }
}
