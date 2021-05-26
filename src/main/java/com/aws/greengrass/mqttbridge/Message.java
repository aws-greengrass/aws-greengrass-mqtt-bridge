/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Common representation of a Message.
 */
@AllArgsConstructor
@EqualsAndHashCode
@Getter
public class Message {
    private String topic;
    private byte[] payload;
}
