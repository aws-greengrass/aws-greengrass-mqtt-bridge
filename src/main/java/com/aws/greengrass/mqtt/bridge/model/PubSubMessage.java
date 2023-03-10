/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.model;

import com.aws.greengrass.builtin.services.pubsub.PublishEvent;
import lombok.NonNull;
import lombok.Value;

@Value
public class PubSubMessage implements Message {

    String topic;
    byte[] payload;

    public static PubSubMessage fromPublishEvent(@NonNull PublishEvent message) {
        return new PubSubMessage(message.getTopic(), message.getPayload());
    }

    @Override
    public Message toPubSub() {
        return this;
    }

    @Override
    public Message toMqtt() {
        return new MqttMessage(topic, payload);
    }

    @Override
    public Message ofTopic(String topic) {
        return new PubSubMessage(topic, payload);
    }
}
