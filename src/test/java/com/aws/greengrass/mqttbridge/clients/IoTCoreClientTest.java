/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge.clients;

import com.aws.greengrass.mqttbridge.Message;
import com.aws.greengrass.mqttclient.CallbackEventManager;
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.mqttclient.PublishRequest;
import com.aws.greengrass.mqttclient.SubscribeRequest;
import com.aws.greengrass.mqttclient.UnsubscribeRequest;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.crt.mqtt.MqttClientConnectionEvents;
import software.amazon.awssdk.crt.mqtt.MqttMessage;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
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

    @Mock
    private ExecutorService mockExecutorService;

    @BeforeEach
    void setup() {
        lenient().doAnswer(invocation -> {
            ((Runnable)invocation.getArgument(0)).run();
            return null;
        }).when(mockExecutorService).submit(any(Runnable.class));
    }

    @Test
    void WHEN_call_iotcore_client_constructed_THEN_does_not_throw() {
        new IoTCoreClient(mockIotMqttClient, mockExecutorService);
    }

    @Test
    void GIVEN_iotcore_client_started_WHEN_update_subscriptions_THEN_topics_subscribed() throws Exception {
        IoTCoreClient iotCoreClient = new IoTCoreClient(mockIotMqttClient, mockExecutorService);
        when(mockIotMqttClient.connected()).thenReturn(true);

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
    void GIVEN_iotcore_client_with_subscriptions_WHEN_call_stop_THEN_topics_unsubscribed() throws Exception {
        IoTCoreClient iotCoreClient = new IoTCoreClient(mockIotMqttClient, mockExecutorService);
        when(mockIotMqttClient.connected()).thenReturn(true);

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
        IoTCoreClient iotCoreClient = new IoTCoreClient(mockIotMqttClient, mockExecutorService);
        when(mockIotMqttClient.connected()).thenReturn(true);

        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");
        iotCoreClient.updateSubscriptions(topics, message -> {
        });

        reset(mockIotMqttClient);
        when(mockIotMqttClient.connected()).thenReturn(true);

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
        IoTCoreClient iotCoreClient = new IoTCoreClient(mockIotMqttClient, mockExecutorService);
        when(mockIotMqttClient.connected()).thenReturn(true);

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
        IoTCoreClient iotCoreClient = new IoTCoreClient(mockIotMqttClient, mockExecutorService);

        byte[] messageFromLocalMqtt = "message from local mqtt".getBytes();

        iotCoreClient.publish(new Message("mapped/topic/from/local/mqtt", messageFromLocalMqtt));

        ArgumentCaptor<PublishRequest> requestCapture = ArgumentCaptor.forClass(PublishRequest.class);
        verify(mockIotMqttClient, times(1)).publish(requestCapture.capture());

        assertThat(requestCapture.getValue().getTopic(), Matchers.is(Matchers.equalTo("mapped/topic/from/local/mqtt")));
        assertThat(requestCapture.getValue().getPayload(), Matchers.is(Matchers.equalTo(messageFromLocalMqtt)));
    }

    @Test
    void GIVEN_iotcore_client_WHEN_update_subscriptions_with_null_message_handler_THEN_throws() {
        IoTCoreClient iotCoreClient = new IoTCoreClient(mockIotMqttClient, mockExecutorService);
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");
        assertThrows(NullPointerException.class, () -> iotCoreClient.updateSubscriptions(topics, null));
    }

    @Test
    void GIVEN_iotcore_client_WHEN_offline_start_and_update_subscriptions_THEN_subscriptions_updated_on_connection()
            throws Exception {
        IoTCoreClient iotCoreClient = new IoTCoreClient(mockIotMqttClient, mockExecutorService);
        when(mockIotMqttClient.connected()).thenReturn(false);
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");
        iotCoreClient.updateSubscriptions(topics, mockMessageHandler);

        verify(mockIotMqttClient, never()).subscribe(any());

        ArgumentCaptor<CallbackEventManager.OnConnectCallback> callbackArgumentCaptor = ArgumentCaptor.forClass(
                CallbackEventManager.OnConnectCallback.class);
        verify(mockIotMqttClient, times(1)).addToCallbackEvents(callbackArgumentCaptor.capture(), any());
        CallbackEventManager.OnConnectCallback connectCallback = callbackArgumentCaptor.getValue();
        connectCallback.onConnect(false);

        ArgumentCaptor<SubscribeRequest> requestArgumentCaptor = ArgumentCaptor.forClass(SubscribeRequest.class);
        verify(mockIotMqttClient, times(2)).subscribe(requestArgumentCaptor.capture());
        List<SubscribeRequest> argValues = requestArgumentCaptor.getAllValues();
        assertThat(argValues.stream().map(SubscribeRequest::getTopic).collect(Collectors.toList()),
                Matchers.containsInAnyOrder("iotcore/topic", "iotcore/topic2"));
        assertThat(iotCoreClient.getSubscribedIotCoreTopics(),
                Matchers.containsInAnyOrder("iotcore/topic", "iotcore/topic2"));
    }

    @Test
    void GIVEN_iotcore_client_WHEN_go_offline_and_update_subscriptions_THEN_subscriptions_updated_on_connection()
            throws Exception {
        IoTCoreClient iotCoreClient = new IoTCoreClient(mockIotMqttClient, mockExecutorService);
        ArgumentCaptor<MqttClientConnectionEvents> connectionCallbackArgumentCaptor = ArgumentCaptor.forClass(
                MqttClientConnectionEvents.class);
        verify(mockIotMqttClient, times(1)).addToCallbackEvents(any(),
                connectionCallbackArgumentCaptor.capture());
        MqttClientConnectionEvents connectionCallback = connectionCallbackArgumentCaptor.getValue();

        connectionCallback.onConnectionInterrupted(0);
        when(mockIotMqttClient.connected()).thenReturn(false);

        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");
        iotCoreClient.updateSubscriptions(topics, mockMessageHandler);

        verify(mockIotMqttClient, never()).subscribe(any());

        connectionCallback.onConnectionResumed(true);

        ArgumentCaptor<SubscribeRequest> requestArgumentCaptor = ArgumentCaptor.forClass(SubscribeRequest.class);
        verify(mockIotMqttClient, times(2)).subscribe(requestArgumentCaptor.capture());
        List<SubscribeRequest> argValues = requestArgumentCaptor.getAllValues();
        assertThat(argValues.stream().map(SubscribeRequest::getTopic).collect(Collectors.toList()),
                Matchers.containsInAnyOrder("iotcore/topic", "iotcore/topic2"));
        assertThat(iotCoreClient.getSubscribedIotCoreTopics(),
                Matchers.containsInAnyOrder("iotcore/topic", "iotcore/topic2"));
    }

    @Test
    void GIVEN_iotcore_client_WHEN_update_subscriptions_and_subscribe_throws_THEN_subscriptions_updated_on_retry(
            ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, ExecutionException.class);

        IoTCoreClient iotCoreClient = new IoTCoreClient(mockIotMqttClient, mockExecutorService);
        when(mockIotMqttClient.connected()).thenReturn(true);

        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");

        doThrow(new ExecutionException(new Exception())).doNothing().when(mockIotMqttClient).subscribe(any());
        iotCoreClient.updateSubscriptions(topics, mockMessageHandler);

        ArgumentCaptor<SubscribeRequest> requestArgumentCaptor = ArgumentCaptor.forClass(SubscribeRequest.class);
        verify(mockIotMqttClient, times(3)).subscribe(requestArgumentCaptor.capture());
        List<SubscribeRequest> argValues = requestArgumentCaptor.getAllValues();
        assertThat(argValues.stream().map(SubscribeRequest::getTopic).collect(Collectors.toList()),
                Matchers.hasItems("iotcore/topic", "iotcore/topic2"));
        assertThat(iotCoreClient.getSubscribedIotCoreTopics(),
                Matchers.containsInAnyOrder("iotcore/topic", "iotcore/topic2"));
    }
}
