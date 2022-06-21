/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge;

import lombok.Value;

/**
 * Common representation of a Message.
 */
@Value
public class Message {
    private String topic;
    private byte[] payload;
}
