package com.aws.iot.evergreen.mqttbridge.auth;

/**
 * Exception thrown in CSR generation.
 */
public class CsrGeneratingException extends Exception {
    static final long serialVersionUID = -3387516993124229948L;

    public CsrGeneratingException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
