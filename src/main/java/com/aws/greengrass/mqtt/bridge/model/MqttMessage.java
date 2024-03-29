/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.model;

import com.aws.greengrass.mqttclient.v5.Publish;
import com.aws.greengrass.mqttclient.v5.UserProperty;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import java.util.Collections;
import java.util.List;

@Value
@Builder(toBuilder = true)
public class MqttMessage implements Message {
    String topic;
    byte[] payload;
    boolean retain;
    Publish.PayloadFormatIndicator payloadFormat;
    String contentType;
    String responseTopic;
    byte[] correlationData;
    @Builder.Default
    List<UserProperty> userProperties = Collections.emptyList();
    Long messageExpiryIntervalSeconds;

    /**
     * Convert spooler mqtt v5 message to an MqttMessage.
     *
     * @param message spooler v5 message
     * @return mqtt message
     */
    public static MqttMessage fromSpoolerV5Model(@NonNull Publish message) {
        return MqttMessage.builder()
                .topic(message.getTopic())
                .payload(message.getPayload())
                .retain(message.isRetain())
                .payloadFormat(message.getPayloadFormat())
                .contentType(message.getContentType())
                .responseTopic(message.getResponseTopic())
                .correlationData(message.getCorrelationData())
                .userProperties(message.getUserProperties())
                .messageExpiryIntervalSeconds(message.getMessageExpiryIntervalSeconds())
                .build();
    }

    /**
     * Convert AWS SDK CRT message to an MqttMessage.
     *
     * @param message crt message
     * @return mqtt message
     */
    public static MqttMessage fromCrtMQTT3(@NonNull software.amazon.awssdk.crt.mqtt.MqttMessage message) {
        return MqttMessage.builder()
                .topic(message.getTopic())
                .payload(message.getPayload())
                .build();
    }

    /**
     * Convert PAHO MQTT3 message to an MqttMessage.
     *
     * @param topic   mqtt topic
     * @param message paho mqtt3 message
     * @return mqtt message
     */
    public static MqttMessage fromPahoMQTT3(@NonNull String topic,
                                            @NonNull org.eclipse.paho.client.mqttv3.MqttMessage message) {
        return MqttMessage.builder()
                .topic(topic)
                .payload(message.getPayload())
                .build();
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
    public Message newFromMessageWithTopic(@NonNull String topic) {
        return toBuilder().topic(topic).build();
    }
}
