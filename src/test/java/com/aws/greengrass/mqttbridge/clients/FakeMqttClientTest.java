/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge.clients;

import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;


@ExtendWith({MockitoExtension.class, GGExtension.class})
public class FakeMqttClientTest {
    final static String T1 = "my/topic";
    final static String T2 = "my/topic/2";
    final static String T3 = "my/topic/3";

    private FakeMqttClient fakeMQTTClient;


    @BeforeEach
    void setup() {
        fakeMQTTClient = new FakeMqttClient("clientId");
    }

    @Test
    void GIVEN_newMqttClient_WHEN_connect_THEN_isConnected() throws MqttException {
        assertThat(fakeMQTTClient.isConnected(), is(false));
        fakeMQTTClient.connect();
        assertThat(fakeMQTTClient.isConnected(), is(true));
    }

    @Test
    void GIVEN_connectedMqttClient_WHEN_disconnect_THEN_isNotConnected() throws MqttException {
        fakeMQTTClient.connect();
        fakeMQTTClient.disconnect();
        assertThat(fakeMQTTClient.isConnected(), is(false));
    }

    @Test
    void GIVEN_mqttClient_WHEN_subscribe_THEN_topicsSubscribed() throws MqttException {
        fakeMQTTClient.subscribe(T1);
        assertThat(fakeMQTTClient.getSubscriptionTopics(), contains(T1));
        fakeMQTTClient.subscribe(T2);
        assertThat(fakeMQTTClient.getSubscriptionTopics(), containsInAnyOrder(T1, T2));
    }

    @Test
    void GIVEN_subscribedMqttClient_WHEN_subscribeWithDuplicateTopicFilter_THEN_subscriptionsUnchanged()
            throws MqttException {
        fakeMQTTClient.subscribe(T1);
        assertThat(fakeMQTTClient.getSubscriptionTopics(), contains(T1));
        fakeMQTTClient.subscribe(T1);
        assertThat(fakeMQTTClient.getSubscriptionTopics(), contains(T1));
    }

    @Test
    void GIVEN_subscribedMqttClient_WHEN_unsubscribe_THEN_topicIsUnsubscribed() throws MqttException {

        fakeMQTTClient.subscribe(T1);
        assertThat(fakeMQTTClient.getSubscriptionTopics(), contains(T1));
        fakeMQTTClient.unsubscribe(T1);
        assertThat(fakeMQTTClient.getSubscriptionTopics(), hasSize(0));

        fakeMQTTClient.subscribe(T1);
        fakeMQTTClient.subscribe(T2);
        fakeMQTTClient.unsubscribe(T1);
        // List is [T2]
        assertThat(fakeMQTTClient.getSubscriptionTopics(), contains(T2));

        fakeMQTTClient.subscribe(T2);
        fakeMQTTClient.subscribe(T3);
        // List is [T2, T3]
        assertThat(fakeMQTTClient.getSubscriptionTopics(), containsInAnyOrder(T2, T3));
        fakeMQTTClient.unsubscribe(T2);
        // List is [T3]
        assertThat(fakeMQTTClient.getSubscriptionTopics(), contains(T3));
    }
}
