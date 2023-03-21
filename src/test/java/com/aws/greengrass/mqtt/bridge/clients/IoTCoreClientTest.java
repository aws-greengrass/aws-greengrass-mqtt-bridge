/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.clients;

import com.aws.greengrass.mqtt.bridge.model.Message;
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.mqttclient.MqttRequestException;
import com.aws.greengrass.mqttclient.spool.SpoolerStoreException;
import com.aws.greengrass.mqttclient.v5.Publish;
import com.aws.greengrass.mqttclient.v5.Subscribe;
import com.aws.greengrass.mqttclient.v5.SubscribeResponse;
import com.aws.greengrass.mqttclient.v5.Unsubscribe;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.TestUtils;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class IoTCoreClientTest {

    @Mock
    private MqttClient mockIotMqttClient;

    @Mock
    private Consumer<com.aws.greengrass.mqtt.bridge.model.MqttMessage> mockMessageHandler;
    private final ExecutorService executorService = TestUtils.synchronousExecutorService();

    @BeforeEach
    void beforeEach() throws Exception {
        resetIotMqttClient();
    }

    private void resetIotMqttClient() throws Exception {
        reset(mockIotMqttClient);

        setOnline(true);
        // TODO fake client
        lenient().when(mockIotMqttClient.subscribe(any(Subscribe.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        new SubscribeResponse("", 0, null)));
        lenient().when(mockIotMqttClient.unsubscribe(any(Unsubscribe.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
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

        ArgumentCaptor<Subscribe> requestArgumentCaptor = ArgumentCaptor.forClass(Subscribe.class);
        verify(mockIotMqttClient, times(2)).subscribe(requestArgumentCaptor.capture());
        List<Subscribe> argValues = requestArgumentCaptor.getAllValues();
        assertThat(argValues.stream().map(Subscribe::getTopic).collect(Collectors.toList()),
                Matchers.containsInAnyOrder("iotcore/topic", "iotcore/topic2"));

        assertThat(iotCoreClient.getSubscribedIotCoreTopics(),
                Matchers.containsInAnyOrder("iotcore/topic", "iotcore/topic2"));
    }

    @Test
    void GIVEN_offline_iotcore_client_WHEN_update_subscriptions_THEN_subscribe_once_online() throws Exception {
        resetIotMqttClient();

        IoTCoreClient iotCoreClient = new IoTCoreClient(mockIotMqttClient, executorService);

        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");

        // attempt to update subscriptions, this is expected to fail since bridge is offline
        setOnline(false);
        iotCoreClient.updateSubscriptions(topics, message -> {
        });

        // verify no subscriptions were made
        verify(mockIotMqttClient, never()).subscribe(any(Subscribe.class));
        assertThat(iotCoreClient.getSubscribedIotCoreTopics().size(), is(0));
        assertThat(iotCoreClient.getToSubscribeIotCoreTopics(),
                Matchers.containsInAnyOrder("iotcore/topic", "iotcore/topic2"));

        // simulate mqtt connection resume
        setOnline(true);
        iotCoreClient.getConnectionCallbacks().onConnectionResumed(false);

        // verify subscriptions were made
        assertThat(iotCoreClient.getSubscribedIotCoreTopics().size(), is(2));
        assertThat(iotCoreClient.getToSubscribeIotCoreTopics(),
                Matchers.containsInAnyOrder("iotcore/topic", "iotcore/topic2"));
    }

    @Test
    void GIVEN_iotcore_client_with_subscriptions_WHEN_call_stop_THEN_topics_Unsubscribed() throws Exception {
        IoTCoreClient iotCoreClient = new IoTCoreClient(mockIotMqttClient, executorService);
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");
        iotCoreClient.updateSubscriptions(topics, message -> {
        });

        iotCoreClient.stop();

        ArgumentCaptor<Unsubscribe> requestArgumentCaptor = ArgumentCaptor.forClass(Unsubscribe.class);
        verify(mockIotMqttClient, times(2)).unsubscribe(requestArgumentCaptor.capture());
        List<Unsubscribe> argValues = requestArgumentCaptor.getAllValues();
        assertThat(argValues.stream().map(Unsubscribe::getTopic).collect(Collectors.toList()),
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

        resetIotMqttClient();

        topics.clear();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2/changed");
        topics.add("iotcore/topic3/added");
        iotCoreClient.updateSubscriptions(topics, message -> {
        });

        ArgumentCaptor<Subscribe> subRequestArgumentCaptor = ArgumentCaptor.forClass(Subscribe.class);
        verify(mockIotMqttClient, times(2)).subscribe(subRequestArgumentCaptor.capture());
        List<Subscribe> subArgValues = subRequestArgumentCaptor.getAllValues();
        assertThat(subArgValues.stream().map(Subscribe::getTopic).collect(Collectors.toList()),
                Matchers.containsInAnyOrder("iotcore/topic2/changed", "iotcore/topic3/added"));

        assertThat(iotCoreClient.getSubscribedIotCoreTopics(), Matchers.hasSize(3));
        assertThat(iotCoreClient.getSubscribedIotCoreTopics(),
                Matchers.containsInAnyOrder("iotcore/topic", "iotcore/topic2/changed", "iotcore/topic3/added"));

        ArgumentCaptor<Unsubscribe> unsubRequestArgumentCaptor
                = ArgumentCaptor.forClass(Unsubscribe.class);
        verify(mockIotMqttClient, times(1)).unsubscribe(unsubRequestArgumentCaptor.capture());
        List<Unsubscribe> unsubArgValues = unsubRequestArgumentCaptor.getAllValues();
        assertThat(unsubArgValues.stream().map(Unsubscribe::getTopic).collect(Collectors.toList()),
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

        ArgumentCaptor<Subscribe> requestArgumentCaptor = ArgumentCaptor.forClass(Subscribe.class);
        verify(mockIotMqttClient, times(2)).subscribe(requestArgumentCaptor.capture());
        Consumer<Publish> iotCoreCallback = requestArgumentCaptor.getValue().getCallback();

        byte[] messageOnTopic1 = "message from topic iotcore/topic".getBytes();
        byte[] messageOnTopic2 = "message from topic iotcore/topic2".getBytes();
        byte[] messageOnTopic3 = "message from topic iotcore/topic/not/in/mapping".getBytes();
        iotCoreCallback.accept(Publish.builder().topic("iotcore/topic").payload(messageOnTopic1).build());
        iotCoreCallback.accept(Publish.builder().topic("iotcore/topic2").payload(messageOnTopic2).build());
        // Also simulate a message which is not in the mapping
        iotCoreCallback.accept(Publish.builder().topic("iotcore/topic/not/in/mapping").payload(messageOnTopic3).build());

        ArgumentCaptor<com.aws.greengrass.mqtt.bridge.model.MqttMessage> messageCapture = ArgumentCaptor.forClass(com.aws.greengrass.mqtt.bridge.model.MqttMessage.class);
        verify(mockMessageHandler, times(3)).accept(messageCapture.capture());

        List<com.aws.greengrass.mqtt.bridge.model.MqttMessage> argValues = messageCapture.getAllValues();
        assertThat(argValues.stream().map(Message::getTopic).collect(Collectors.toList()),
                Matchers.containsInAnyOrder("iotcore/topic", "iotcore/topic2", "iotcore/topic/not/in/mapping"));
        assertThat(argValues.stream().map(Message::getPayload).collect(Collectors.toList()),
                Matchers.containsInAnyOrder(messageOnTopic1, messageOnTopic2, messageOnTopic3));
    }

    @Test
    void GIVEN_iotcore_client_and_subscribed_WHEN_published_message_THEN_routed_to_iotcore_mqttclient() throws MessageClientException, MqttRequestException, SpoolerStoreException, InterruptedException {
        IoTCoreClient iotCoreClient = new IoTCoreClient(mockIotMqttClient, executorService);
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");
        iotCoreClient.updateSubscriptions(topics, message -> {
        });

        byte[] messageFromLocalMqtt = "message from local mqtt".getBytes();

        iotCoreClient.publish(com.aws.greengrass.mqtt.bridge.model.MqttMessage.builder()
                .topic("mapped/topic/from/local/mqtt")
                .payload(messageFromLocalMqtt)
                .build());

        ArgumentCaptor<Publish> requestCapture = ArgumentCaptor.forClass(Publish.class);
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

    private void setOnline(boolean online) {
        lenient().when(mockIotMqttClient.getMqttOnline()).thenReturn(new AtomicBoolean(online));
    }
}
