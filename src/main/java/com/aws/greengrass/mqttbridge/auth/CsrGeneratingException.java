/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge.auth;

/**
 * Exception thrown in CSR generation.
 */
public class CsrGeneratingException extends Exception {
    static final long serialVersionUID = -3387516993124229948L;

    public CsrGeneratingException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
