/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.clients;

import com.aws.greengrass.mqtt.bridge.Message;
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.mqttclient.PublishRequest;
import com.aws.greengrass.mqttclient.SubscribeRequest;
import com.aws.greengrass.mqttclient.UnsubscribeRequest;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.TestUtils;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.crt.mqtt.MqttMessage;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class IoTCoreClientTest {

    @Mock
    private MqttClient mockIotMqttClient;

    @Mock
    private Consumer<Message> mockMessageHandler;
    private final ExecutorService executorService = TestUtils.synchronousExecutorService();

    @BeforeEach
    void beforeEach() {
        lenient().when(mockIotMqttClient.connected()).thenReturn(true);
    }

    @Test
    void WHEN_call_iotcore_client_constructed_THEN_does_not_throw() {
        new IoTCoreClient(mockIotMqttClient, executorService);
    }

    @Test
    void GIVEN_iotcore_client_started_WHEN_update_subscriptions_THEN_topics_subscribed() throws Exception {
        IoTCoreClient iotCoreClient = new IoTCoreClient(mockIotMqttClient, executorService);
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");
        iotCoreClient.updateSubscriptions(topics, message -> {
        });

        ArgumentCaptor<SubscribeRequest> requestArgumentCaptor = ArgumentCaptor.forClass(SubscribeRequest.class);
        verify(mockIotMqttClient, times(2)).subscribe(requestArgumentCaptor.capture());
        List<SubscribeRequest> argValues = requestArgumentCaptor.getAllValues();
        assertThat(argValues.stream().map(SubscribeRequest::getTopic).collect(Collectors.toList()),
                Matchers.containsInAnyOrder("iotcore/topic", "iotcore/topic2"));

        assertThat(iotCoreClient.getSubscribedIotCoreTopics(),
                Matchers.containsInAnyOrder("iotcore/topic", "iotcore/topic2"));
    }

    @Test
    void GIVEN_offline_iotcore_client_WHEN_update_subscriptions_THEN_subscribe_once_online() {
        reset(mockIotMqttClient);

        IoTCoreClient iotCoreClient = new IoTCoreClient(mockIotMqttClient, executorService);

        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");

        // attempt to update subscriptions, this is expected to fail since bridge is offline
        when(mockIotMqttClient.connected()).thenReturn(false);
        iotCoreClient.updateSubscriptions(topics, message -> {
        });

        // verify no subscriptions were made
        assertThat(iotCoreClient.getSubscribedIotCoreTopics().size(), is(0));
        assertThat(iotCoreClient.getToSubscribeIotCoreTopics(),
                Matchers.containsInAnyOrder("iotcore/topic", "iotcore/topic2"));

        // simulate mqtt connection resume
        when(mockIotMqttClient.connected()).thenReturn(true);
        iotCoreClient.getConnectionCallbacks().onConnectionResumed(false);

        // verify subscriptions were made
        assertThat(iotCoreClient.getSubscribedIotCoreTopics().size(), is(2));
        assertThat(iotCoreClient.getToSubscribeIotCoreTopics(),
                Matchers.containsInAnyOrder("iotcore/topic", "iotcore/topic2"));
    }

    @Test
    void GIVEN_iotcore_client_with_subscriptions_WHEN_call_stop_THEN_topics_unsubscribed() throws Exception {
        IoTCoreClient iotCoreClient = new IoTCoreClient(mockIotMqttClient, executorService);
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");
        iotCoreClient.updateSubscriptions(topics, message -> {
        });

        iotCoreClient.stop();

        ArgumentCaptor<UnsubscribeRequest> requestArgumentCaptor = ArgumentCaptor.forClass(UnsubscribeRequest.class);
        verify(mockIotMqttClient, times(2)).unsubscribe(requestArgumentCaptor.capture());
        List<UnsubscribeRequest> argValues = requestArgumentCaptor.getAllValues();
        assertThat(argValues.stream().map(UnsubscribeRequest::getTopic).collect(Collectors.toList()),
                Matchers.containsInAnyOrder("iotcore/topic", "iotcore/topic2"));

        assertThat(iotCoreClient.getSubscribedIotCoreTopics(), Matchers.hasSize(0));
    }

    @Test
    void GIVEN_iotcore_client_with_subscriptions_WHEN_subscriptions_updated_THEN_subscriptions_updated()
            throws Exception {
        IoTCoreClient iotCoreClient = new IoTCoreClient(mockIotMqttClient, executorService);
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");
        iotCoreClient.updateSubscriptions(topics, message -> {
        });

        reset(mockIotMqttClient);
        lenient().when(mockIotMqttClient.connected()).thenReturn(true);

        topics.clear();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2/changed");
        topics.add("iotcore/topic3/added");
        iotCoreClient.updateSubscriptions(topics, message -> {
        });

        ArgumentCaptor<SubscribeRequest> subRequestArgumentCaptor = ArgumentCaptor.forClass(SubscribeRequest.class);
        verify(mockIotMqttClient, times(2)).subscribe(subRequestArgumentCaptor.capture());
        List<SubscribeRequest> subArgValues = subRequestArgumentCaptor.getAllValues();
        assertThat(subArgValues.stream().map(SubscribeRequest::getTopic).collect(Collectors.toList()),
                Matchers.containsInAnyOrder("iotcore/topic2/changed", "iotcore/topic3/added"));

        assertThat(iotCoreClient.getSubscribedIotCoreTopics(), Matchers.hasSize(3));
        assertThat(iotCoreClient.getSubscribedIotCoreTopics(),
                Matchers.containsInAnyOrder("iotcore/topic", "iotcore/topic2/changed", "iotcore/topic3/added"));

        ArgumentCaptor<UnsubscribeRequest> unsubRequestArgumentCaptor
                = ArgumentCaptor.forClass(UnsubscribeRequest.class);
        verify(mockIotMqttClient, times(1)).unsubscribe(unsubRequestArgumentCaptor.capture());
        List<UnsubscribeRequest> unsubArgValues = unsubRequestArgumentCaptor.getAllValues();
        assertThat(unsubArgValues.stream().map(UnsubscribeRequest::getTopic).collect(Collectors.toList()),
                Matchers.containsInAnyOrder("iotcore/topic2"));
    }

    @Test
    void GIVEN_iotcore_client_and_subscribed_WHEN_receive_iotcore_message_THEN_routed_to_message_handler()
            throws Exception {
        IoTCoreClient iotCoreClient = new IoTCoreClient(mockIotMqttClient, executorService);
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");
        iotCoreClient.updateSubscriptions(topics, mockMessageHandler);

        ArgumentCaptor<SubscribeRequest> requestArgumentCaptor = ArgumentCaptor.forClass(SubscribeRequest.class);
        verify(mockIotMqttClient, times(2)).subscribe(requestArgumentCaptor.capture());
        Consumer<MqttMessage> iotCoreCallback = requestArgumentCaptor.getValue().getCallback();

        byte[] messageOnTopic1 = "message from topic iotcore/topic".getBytes();
        byte[] messageOnTopic2 = "message from topic iotcore/topic2".getBytes();
        byte[] messageOnTopic3 = "message from topic iotcore/topic/not/in/mapping".getBytes();
        iotCoreCallback.accept(new MqttMessage("iotcore/topic", messageOnTopic1));
        iotCoreCallback.accept(new MqttMessage("iotcore/topic2", messageOnTopic2));
        // Also simulate a message which is not in the mapping
        iotCoreCallback.accept(new MqttMessage("iotcore/topic/not/in/mapping", messageOnTopic3));

        ArgumentCaptor<Message> messageCapture = ArgumentCaptor.forClass(Message.class);
        verify(mockMessageHandler, times(3)).accept(messageCapture.capture());

        List<Message> argValues = messageCapture.getAllValues();
        assertThat(argValues.stream().map(Message::getTopic).collect(Collectors.toList()),
                Matchers.containsInAnyOrder("iotcore/topic", "iotcore/topic2", "iotcore/topic/not/in/mapping"));
        assertThat(argValues.stream().map(Message::getPayload).collect(Collectors.toList()),
                Matchers.containsInAnyOrder(messageOnTopic1, messageOnTopic2, messageOnTopic3));
    }

    @Test
    void GIVEN_iotcore_client_and_subscribed_WHEN_published_message_THEN_routed_to_iotcore_mqttclient() {
        IoTCoreClient iotCoreClient = new IoTCoreClient(mockIotMqttClient, executorService);
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");
        iotCoreClient.updateSubscriptions(topics, message -> {
        });

        byte[] messageFromLocalMqtt = "message from local mqtt".getBytes();

        iotCoreClient.publish(new Message("mapped/topic/from/local/mqtt", messageFromLocalMqtt));

        ArgumentCaptor<PublishRequest> requestCapture = ArgumentCaptor.forClass(PublishRequest.class);
        verify(mockIotMqttClient, times(1)).publish(requestCapture.capture());

        assertThat(requestCapture.getValue().getTopic(), is(Matchers.equalTo("mapped/topic/from/local/mqtt")));
        assertThat(requestCapture.getValue().getPayload(), is(Matchers.equalTo(messageFromLocalMqtt)));
    }

    @Test
    void GIVEN_iotcore_client_WHEN_update_subscriptions_with_null_message_handler_THEN_throws() {
        IoTCoreClient iotCoreClient = new IoTCoreClient(mockIotMqttClient, executorService);
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");
        assertThrows(NullPointerException.class, () -> iotCoreClient.updateSubscriptions(topics, null));
    }
}
