/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.iot.evergreen.mqtt.bridge;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Common representation of a Message.
 */
@AllArgsConstructor
@Getter
public class Message {
    private String topic;
    private byte[] payload;
}
