/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.model;


import com.aws.greengrass.mqttclient.v5.Publish;
import com.aws.greengrass.mqttclient.v5.UserProperty;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.crt.mqtt.QualityOfService;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class MqttMessageTest {

    @Test
    void GIVEN_mqtt3_content_WHEN_mqtt_message_created_THEN_success() {
        String topic = "topic";
        byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
        MqttMessage v3 = MqttMessage.builder().topic(topic).payload(payload).build();
        MqttMessage expected = MqttMessage.builder()
                .topic(topic)
                .payload(payload)
                .messageExpiryIntervalSeconds(null)
                .contentType(null)
                .correlationData(null)
                .payloadFormat(null)
                .retain(false)
                .responseTopic(null)
                .userProperties(Collections.emptyList())
                .build();
        assertEquals(expected, v3);
    }

    @Test
    void GIVEN_paho_v3_message_WHEN_mqtt_message_created_THEN_success() {
        String topic = "topic";
        byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
        MqttMessage v3 = MqttMessage.fromPahoMQTT3(topic,
                new org.eclipse.paho.client.mqttv3.MqttMessage(payload));
        MqttMessage expected = MqttMessage.builder()
                .topic(topic)
                .payload(payload)
                .messageExpiryIntervalSeconds(null)
                .contentType(null)
                .correlationData(null)
                .payloadFormat(null)
                .retain(false)
                .responseTopic(null)
                .userProperties(Collections.emptyList())
                .build();
        assertEquals(expected, v3);
    }

    @Test
    void GIVEN_crt_v3_message_WHEN_mqtt_message_created_THEN_success() {
        String topic = "topic";
        byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
        MqttMessage v3 = MqttMessage.fromCrtMQTT3(
                new software.amazon.awssdk.crt.mqtt.MqttMessage(
                        topic, payload, QualityOfService.AT_LEAST_ONCE, true));
        MqttMessage expected = MqttMessage.builder()
                .topic(topic)
                .payload(payload)
                .messageExpiryIntervalSeconds(null)
                .contentType(null)
                .correlationData(null)
                .payloadFormat(null)
                .retain(false)
                .responseTopic(null)
                .userProperties(Collections.emptyList())
                .build();
        assertEquals(expected, v3);
    }

    @Test
    void GIVEN_mqtt_message_v3_WHEN_convert_to_mqtt_message_THEN_success() {
        String topic = "topic";
        byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
        MqttMessage v3 = MqttMessage.builder().topic(topic).payload(payload).build();
        assertEquals(v3, v3.toMqtt());
    }

    @Test
    void GIVEN_mqtt_message_v3_WHEN_convert_to_pubsub_THEN_success() {
        String topic = "topic";
        byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
        MqttMessage v3 = MqttMessage.builder().topic(topic).payload(payload).build();
        assertEquals(new PubSubMessage(topic, payload), v3.toPubSub());
    }

    @Test
    void GIVEN_mqtt_message_v3_WHEN_new_message_with_topic_THEN_success() {
        String newTopic = "newTopic";
        byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
        MqttMessage v3 = MqttMessage.builder().topic("topic").payload(payload).build();
        MqttMessage expected = MqttMessage.builder().topic(newTopic).payload(payload).build();
        assertEquals(expected, v3.newFromMessageWithTopic(newTopic));
    }

    @Test
    void GIVEN_mqtt5_content_WHEN_mqtt_message_created_THEN_success() {
        String topic = "topic";
        byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
        Long expiryInterval = 5L;
        String contentType = "text/plain";
        byte[] correlationData = "data".getBytes(StandardCharsets.UTF_8);
        Publish.PayloadFormatIndicator payloadFormat = Publish.PayloadFormatIndicator.UTF8;
        boolean retain = true;
        String responseTopic = "topic";
        List<UserProperty> userProperties = Collections.singletonList(new UserProperty("key", "val"));

        MqttMessage v5 = MqttMessage.builder()
                .topic(topic)
                .payload(payload)
                .messageExpiryIntervalSeconds(expiryInterval)
                .contentType(contentType)
                .correlationData(correlationData)
                .payloadFormat(payloadFormat)
                .retain(retain)
                .responseTopic(responseTopic)
                .userProperties(userProperties)
                .build();

        assertEquals(topic, v5.getTopic());
        assertEquals(payload, v5.getPayload());
        assertEquals(expiryInterval, v5.getMessageExpiryIntervalSeconds());
        assertEquals(contentType, v5.getContentType());
        assertEquals(correlationData, v5.getCorrelationData());
        assertEquals(payloadFormat, v5.getPayloadFormat());
        assertEquals(retain, v5.isRetain());
        assertEquals(responseTopic, v5.getResponseTopic());
        assertEquals(userProperties, v5.getUserProperties());
    }

    @Test
    void GIVEN_mqtt_message_v5_WHEN_convert_to_mqtt_message_THEN_success() {
        MqttMessage v5 = MqttMessage.builder()
                .topic("topic")
                .payload("payload".getBytes(StandardCharsets.UTF_8))
                .messageExpiryIntervalSeconds(5L)
                .contentType("text/plain")
                .correlationData("data".getBytes(StandardCharsets.UTF_8))
                .payloadFormat(Publish.PayloadFormatIndicator.UTF8)
                .retain(true)
                .responseTopic("topic")
                .userProperties(Collections.singletonList(new UserProperty("key", "val")))
                .build();
        assertEquals(v5, v5.toMqtt());
    }

    @Test
    void GIVEN_mqtt_message_v5_WHEN_convert_to_pubsub_THEN_success() {
        String topic = "topic";
        byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);

        MqttMessage v5 = MqttMessage.builder()
                .topic(topic)
                .payload(payload)
                .messageExpiryIntervalSeconds(5L)
                .contentType("text/plain")
                .correlationData("data".getBytes(StandardCharsets.UTF_8))
                .payloadFormat(Publish.PayloadFormatIndicator.UTF8)
                .retain(true)
                .responseTopic("topic")
                .userProperties(Collections.singletonList(new UserProperty("key", "val")))
                .build();
        assertEquals(new PubSubMessage(topic, payload), v5.toPubSub());
    }

    @Test
    void GIVEN_mqtt_message_v5_WHEN_new_message_with_topic_THEN_success() {
        String newTopic = "newTopic";

        String topic = "topic";
        byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
        Long expiryInterval = 5L;
        String contentType = "text/plain";
        byte[] correlationData = "data".getBytes(StandardCharsets.UTF_8);
        Publish.PayloadFormatIndicator payloadFormat = Publish.PayloadFormatIndicator.UTF8;
        boolean retain = true;
        String responseTopic = "topic";
        List<UserProperty> userProperties = Collections.singletonList(new UserProperty("key", "val"));

        MqttMessage v5 = MqttMessage.builder()
                .topic(topic)
                .payload(payload)
                .messageExpiryIntervalSeconds(expiryInterval)
                .contentType(contentType)
                .correlationData(correlationData)
                .payloadFormat(payloadFormat)
                .retain(retain)
                .responseTopic(responseTopic)
                .userProperties(userProperties)
                .build();

        MqttMessage expected = MqttMessage.builder()
                .topic(newTopic)
                .payload(payload)
                .messageExpiryIntervalSeconds(expiryInterval)
                .contentType(contentType)
                .correlationData(correlationData)
                .payloadFormat(payloadFormat)
                .retain(retain)
                .responseTopic(responseTopic)
                .userProperties(userProperties)
                .build();

        assertEquals(expected, v5.newFromMessageWithTopic(newTopic));
    }
}
