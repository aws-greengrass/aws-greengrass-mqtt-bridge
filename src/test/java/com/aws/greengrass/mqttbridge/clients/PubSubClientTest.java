/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge.clients;

import com.aws.greengrass.builtin.services.pubsub.PubSubIPCEventStreamAgent;
import com.aws.greengrass.builtin.services.pubsub.PublishEvent;
import com.aws.greengrass.mqttbridge.Message;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.aws.greengrass.mqttbridge.MQTTBridge.SERVICE_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class PubSubClientTest {

    @Mock
    private PubSubIPCEventStreamAgent mockPubSubIPCAgent;

    @Mock
    private Consumer<Message> mockMessageHandler;

    @Test
    void WHEN_call_pubsub_client_constructed_THEN_does_not_throw() {
        new PubSubClient(mockPubSubIPCAgent);
    }

    @Test
    void GIVEN_pubsub_client_started_WHEN_update_subscriptions_THEN_topics_subscribed() {
        PubSubClient pubSubClient = new PubSubClient(mockPubSubIPCAgent);
        Set<String> topics = new HashSet<>();
        topics.add("pubsub/topic");
        topics.add("pubsub/topic2");
        pubSubClient.updateSubscriptions(topics, message -> {
        });

        ArgumentCaptor<String> topicArgumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockPubSubIPCAgent, times(2)).subscribe(topicArgumentCaptor.capture(),
                any(), eq(SERVICE_NAME));
        List<String> argValues = topicArgumentCaptor.getAllValues();
        assertThat(argValues, Matchers.containsInAnyOrder("pubsub/topic", "pubsub/topic2"));

        assertThat(pubSubClient.getSubscribedPubSubTopics(),
                Matchers.containsInAnyOrder("pubsub/topic", "pubsub/topic2"));
    }

    @Test
    void GIVEN_pubsub_client_with_subscriptions_WHEN_call_stop_THEN_topics_unsubscribed() {
        PubSubClient pubSubClient = new PubSubClient(mockPubSubIPCAgent);
        Set<String> topics = new HashSet<>();
        topics.add("pubsub/topic");
        topics.add("pubsub/topic2");
        pubSubClient.updateSubscriptions(topics, message -> {
        });

        pubSubClient.stop();

        ArgumentCaptor<String> topicArgumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockPubSubIPCAgent, times(2)).unsubscribe(topicArgumentCaptor.capture(),
                any(), eq(SERVICE_NAME));
        List<String> argValues = topicArgumentCaptor.getAllValues();
        assertThat(argValues, Matchers.containsInAnyOrder("pubsub/topic", "pubsub/topic2"));

        assertThat(pubSubClient.getSubscribedPubSubTopics(), Matchers.hasSize(0));
    }

    @Test
    void GIVEN_pubsub_client_with_subscriptions_WHEN_subscriptions_updated_THEN_subscriptions_updated() {
        PubSubClient pubSubClient = new PubSubClient(mockPubSubIPCAgent);
        Set<String> topics = new HashSet<>();
        topics.add("pubsub/topic");
        topics.add("pubsub/topic2");
        pubSubClient.updateSubscriptions(topics, message -> {
        });

        reset(mockPubSubIPCAgent);

        topics.clear();
        topics.add("pubsub/topic");
        topics.add("pubsub/topic2/changed");
        topics.add("pubsub/topic3/added");
        pubSubClient.updateSubscriptions(topics, message -> {
        });

        ArgumentCaptor<String> subTopicArgumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockPubSubIPCAgent, times(2)).subscribe(subTopicArgumentCaptor.capture(),
                any(), eq(SERVICE_NAME));
        List<String> subArgValues = subTopicArgumentCaptor.getAllValues();
        assertThat(subArgValues, Matchers.containsInAnyOrder("pubsub/topic2/changed", "pubsub/topic3/added"));

        assertThat(pubSubClient.getSubscribedPubSubTopics(), Matchers.hasSize(3));
        assertThat(pubSubClient.getSubscribedPubSubTopics(),
                Matchers.containsInAnyOrder("pubsub/topic", "pubsub/topic2/changed", "pubsub/topic3/added"));

        ArgumentCaptor<String> unsubTopicArgumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockPubSubIPCAgent, times(1)).unsubscribe(unsubTopicArgumentCaptor.capture(),
                any(), eq(SERVICE_NAME));
        List<String> unsubArgValues = unsubTopicArgumentCaptor.getAllValues();
        assertThat(unsubArgValues, Matchers.containsInAnyOrder("pubsub/topic2"));
    }

    @Test
    void GIVEN_pubsub_client_and_subscribed_WHEN_receive_pubsub_message_THEN_routed_to_message_handler() {
        PubSubClient pubSubClient = new PubSubClient(mockPubSubIPCAgent);
        Set<String> topics = new HashSet<>();
        topics.add("pubsub/topic");
        topics.add("pubsub/topic2");
        pubSubClient.updateSubscriptions(topics, mockMessageHandler);

        ArgumentCaptor<Consumer<PublishEvent>> cbArgumentCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(mockPubSubIPCAgent, times(2)).subscribe(any(), cbArgumentCaptor.capture(),
                eq(SERVICE_NAME));
        Consumer<PublishEvent> pubsubCallback = cbArgumentCaptor.getValue();

        byte[] messageOnTopic1 = "message from topic pubsub/topic".getBytes();
        byte[] messageOnTopic2 = "message from topic pubsub/topic2".getBytes();
        byte[] messageOnTopic3 = "message from topic pubsub/topic/not/in/mapping".getBytes();
        pubsubCallback.accept(PublishEvent.builder().topic("pubsub/topic").payload(messageOnTopic1).build());
        pubsubCallback.accept(PublishEvent.builder().topic("pubsub/topic2").payload(messageOnTopic2).build());
        // Also simulate a message which is not in the mapping
        pubsubCallback.accept(PublishEvent.builder().topic("pubsub/topic/not/in/mapping")
                .payload(messageOnTopic3).build());

        ArgumentCaptor<Message> messageCapture = ArgumentCaptor.forClass(Message.class);
        verify(mockMessageHandler, times(3)).accept(messageCapture.capture());

        List<Message> argValues = messageCapture.getAllValues();
        assertThat(argValues.stream().map(Message::getTopic).collect(Collectors.toList()),
                Matchers.containsInAnyOrder("pubsub/topic", "pubsub/topic2", "pubsub/topic/not/in/mapping"));
        assertThat(argValues.stream().map(Message::getPayload).collect(Collectors.toList()),
                Matchers.containsInAnyOrder(messageOnTopic1, messageOnTopic2, messageOnTopic3));
    }

    @Test
    void GIVEN_pubsub_client_and_subscribed_WHEN_published_message_THEN_routed_to_pubsub_ipcagent() {
        PubSubClient pubSubClient = new PubSubClient(mockPubSubIPCAgent);
        Set<String> topics = new HashSet<>();
        topics.add("pubsub/topic");
        topics.add("pubsub/topic2");
        pubSubClient.updateSubscriptions(topics, message -> {
        });

        byte[] messageFromLocalMqtt = "message from local mqtt".getBytes();

        pubSubClient.publish(new Message("mapped/topic/from/local/mqtt", messageFromLocalMqtt));

        ArgumentCaptor<String> topicCapture = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<byte[]> payloadCapture = ArgumentCaptor.forClass(byte[].class);
        verify(mockPubSubIPCAgent, times(1)).publish(topicCapture.capture(),
                payloadCapture.capture(), eq(SERVICE_NAME));

        assertThat(topicCapture.getValue(), Matchers.is(Matchers.equalTo("mapped/topic/from/local/mqtt")));
        assertThat(payloadCapture.getValue(), Matchers.is(Matchers.equalTo(messageFromLocalMqtt)));
    }

    @Test
    void GIVEN_pubsub_client_WHEN_update_subscriptions_with_null_message_handler_THEN_throws() {
        PubSubClient pubSubClient = new PubSubClient(mockPubSubIPCAgent);
        Set<String> topics = new HashSet<>();
        topics.add("pubsub/topic");
        topics.add("pubsub/topic2");
        assertThrows(NullPointerException.class, () -> pubSubClient.updateSubscriptions(topics, null));
    }
}
