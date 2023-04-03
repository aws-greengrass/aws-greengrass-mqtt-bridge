/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.clients;

/**
 * Exception thrown by the local mqtt5 client to retry operations.
 */
public class RetryableMqttOperationException extends MessageClientException {
    static final long serialVersionUID = -3387516993124229948L;

    RetryableMqttOperationException(String msg, Throwable cause) {
        super(msg, cause);
    }

    RetryableMqttOperationException(String msg) {
        super(msg);
    }
}
