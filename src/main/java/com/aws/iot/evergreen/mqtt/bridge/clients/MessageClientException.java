/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.iot.evergreen.mqtt.bridge.clients;

/**
 * Exception thrown by the Message Clients.
 */
public class MessageClientException extends Exception {
    static final long serialVersionUID = -3387516993124229948L;

    /**
     * Ctr for {@link MessageClientException}.
     *
     * @param msg   message
     * @param cause cause of the exception
     */
    public MessageClientException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Ctr for {@link MessageClientException}.
     *
     * @param msg message
     */
    public MessageClientException(String msg) {
        super(msg);
    }
}