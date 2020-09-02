package com.aws.iot.evergreen.mqtt.bridge.clients;

import com.aws.iot.evergreen.builtin.services.pubsub.PubSubIPCAgent;
import com.aws.iot.evergreen.ipc.services.pubsub.MessagePublishedEvent;
import com.aws.iot.evergreen.ipc.services.pubsub.PubSubPublishRequest;
import com.aws.iot.evergreen.ipc.services.pubsub.PubSubSubscribeRequest;
import com.aws.iot.evergreen.ipc.services.pubsub.PubSubUnsubscribeRequest;
import com.aws.iot.evergreen.mqtt.bridge.Message;
import com.aws.iot.evergreen.testcommons.testutilities.EGExtension;
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith({MockitoExtension.class, EGExtension.class})
public class PubSubClientTest {

    @Mock
    private PubSubIPCAgent mockPubSubIPCAgent;

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

        ArgumentCaptor<PubSubSubscribeRequest> requestArgumentCaptor
                = ArgumentCaptor.forClass(PubSubSubscribeRequest.class);
        verify(mockPubSubIPCAgent, times(2)).subscribe(requestArgumentCaptor.capture(),
                (Consumer<MessagePublishedEvent>) any());
        List<PubSubSubscribeRequest> argValues = requestArgumentCaptor.getAllValues();
        assertThat(argValues.stream().map(PubSubSubscribeRequest::getTopic).collect(Collectors.toList()),
                Matchers.containsInAnyOrder("pubsub/topic", "pubsub/topic2"));

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

        ArgumentCaptor<PubSubUnsubscribeRequest> requestArgumentCaptor
                = ArgumentCaptor.forClass(PubSubUnsubscribeRequest.class);
        verify(mockPubSubIPCAgent, times(2)).unsubscribe(requestArgumentCaptor.capture(),
                (Consumer<MessagePublishedEvent>) any());
        List<PubSubUnsubscribeRequest> argValues = requestArgumentCaptor.getAllValues();
        assertThat(argValues.stream().map(PubSubUnsubscribeRequest::getTopic).collect(Collectors.toList()),
                Matchers.containsInAnyOrder("pubsub/topic", "pubsub/topic2"));

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

        ArgumentCaptor<PubSubSubscribeRequest> subRequestArgumentCaptor
                = ArgumentCaptor.forClass(PubSubSubscribeRequest.class);
        verify(mockPubSubIPCAgent, times(2)).subscribe(subRequestArgumentCaptor.capture(),
                (Consumer<MessagePublishedEvent>) any());
        List<PubSubSubscribeRequest> subArgValues = subRequestArgumentCaptor.getAllValues();
        assertThat(subArgValues.stream().map(PubSubSubscribeRequest::getTopic).collect(Collectors.toList()),
                Matchers.containsInAnyOrder("pubsub/topic2/changed", "pubsub/topic3/added"));

        assertThat(pubSubClient.getSubscribedPubSubTopics(), Matchers.hasSize(3));
        assertThat(pubSubClient.getSubscribedPubSubTopics(),
                Matchers.containsInAnyOrder("pubsub/topic", "pubsub/topic2/changed", "pubsub/topic3/added"));

        ArgumentCaptor<PubSubUnsubscribeRequest> unsubRequestArgumentCaptor
                = ArgumentCaptor.forClass(PubSubUnsubscribeRequest.class);
        verify(mockPubSubIPCAgent, times(1)).unsubscribe(unsubRequestArgumentCaptor.capture(),
                (Consumer<MessagePublishedEvent>) any());
        List<PubSubUnsubscribeRequest> unsubArgValues = unsubRequestArgumentCaptor.getAllValues();
        assertThat(unsubArgValues.stream().map(PubSubUnsubscribeRequest::getTopic).collect(Collectors.toList()),
                Matchers.containsInAnyOrder("pubsub/topic2"));
    }

    @Test
    void GIVEN_pubsub_client_and_subscribed_WHEN_receive_pubsub_message_THEN_routed_to_message_handler() {
        PubSubClient pubSubClient = new PubSubClient(mockPubSubIPCAgent);
        Set<String> topics = new HashSet<>();
        topics.add("pubsub/topic");
        topics.add("pubsub/topic2");
        pubSubClient.updateSubscriptions(topics, mockMessageHandler);

        ArgumentCaptor<Consumer<MessagePublishedEvent>> cbArgumentCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(mockPubSubIPCAgent, times(2)).subscribe(any(), cbArgumentCaptor.capture());
        Consumer<MessagePublishedEvent> pubsubCallback = cbArgumentCaptor.getValue();

        byte[] messageOnTopic1 = "message from topic pubsub/topic".getBytes();
        byte[] messageOnTopic2 = "message from topic pubsub/topic2".getBytes();
        byte[] messageOnTopic3 = "message from topic pubsub/topic/not/in/mapping".getBytes();
        pubsubCallback.accept(MessagePublishedEvent.builder().topic("pubsub/topic").payload(messageOnTopic1).build());
        pubsubCallback.accept(MessagePublishedEvent.builder().topic("pubsub/topic2").payload(messageOnTopic2).build());
        // Also simulate a message which is not in the mapping
        pubsubCallback.accept(MessagePublishedEvent.builder().topic("pubsub/topic/not/in/mapping")
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

        ArgumentCaptor<PubSubPublishRequest> requestCapture = ArgumentCaptor.forClass(PubSubPublishRequest.class);
        verify(mockPubSubIPCAgent, times(1)).publish(requestCapture.capture());

        assertThat(requestCapture.getValue().getTopic(), Matchers.is(Matchers.equalTo("mapped/topic/from/local/mqtt")));
        assertThat(requestCapture.getValue().getPayload(), Matchers.is(Matchers.equalTo(messageFromLocalMqtt)));
    }
}
