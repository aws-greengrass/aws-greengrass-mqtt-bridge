package com.aws.greengrass.mqttbridge.clients;

public class SubscribeInterruptedException extends Exception {
    static final long serialVersionUID = -3387516993124229948L;

    /**
     * Ctr for {@link SubscribeInterruptedException}.
     *
     * @param cause cause of the exception
     */
    public SubscribeInterruptedException(Throwable cause) {
        super(cause);
    }
}
