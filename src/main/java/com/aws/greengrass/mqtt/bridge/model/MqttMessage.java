/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.model;

import lombok.NonNull;
import lombok.Value;

// TODO support mqtt5
@Value
public class MqttMessage implements Message {
    String topic;
    byte[] payload;

    public static MqttMessage fromCrtMQTT3(@NonNull software.amazon.awssdk.crt.mqtt.MqttMessage message) {
        return new MqttMessage(message.getTopic(), message.getPayload());
    }

    public static MqttMessage fromPahoMQTT3(@NonNull String topic,
                                            @NonNull org.eclipse.paho.client.mqttv3.MqttMessage message) {
        return new MqttMessage(topic, message.getPayload());
    }

    @Override
    public Message toPubSub() {
        return new PubSubMessage(topic, payload);
    }

    @Override
    public Message toMqtt() {
        return this;
    }

    @Override
    public Message ofTopic(String topic) {
        return new MqttMessage(topic, payload);
    }
}
