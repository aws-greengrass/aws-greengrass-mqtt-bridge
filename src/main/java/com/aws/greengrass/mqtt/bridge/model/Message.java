/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.model;

/**
 * Common representation of a Message.
 */
public interface Message {
    Message toPubSub();

    Message toMqtt();

    String getTopic();

    byte[] getPayload();

    Message ofTopic(String topic);
}
