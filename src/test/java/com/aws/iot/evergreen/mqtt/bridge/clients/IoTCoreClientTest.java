package com.aws.iot.evergreen.mqtt.bridge.clients;

import com.aws.iot.evergreen.mqtt.MqttClient;
import com.aws.iot.evergreen.mqtt.PublishRequest;
import com.aws.iot.evergreen.mqtt.SubscribeRequest;
import com.aws.iot.evergreen.mqtt.UnsubscribeRequest;
import com.aws.iot.evergreen.mqtt.bridge.Message;
import com.aws.iot.evergreen.testcommons.testutilities.EGExtension;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.crt.mqtt.MqttMessage;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith({MockitoExtension.class, EGExtension.class})
public class IoTCoreClientTest {

    @Mock
    private MqttClient mockIotMqttClient;

    @Mock
    private Consumer<Message> mockMessageHandler;

    @Test
    void WHEN_call_iotcore_client_constructed_THEN_does_not_throw() {
        new IoTCoreClient(mockIotMqttClient);
    }

    @Test
    void GIVEN_iotcore_client_started_WHEN_update_subscriptions_THEN_topics_subscribed() throws Exception {
        IoTCoreClient iotCoreClient = new IoTCoreClient(mockIotMqttClient);
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
        IoTCoreClient iotCoreClient = new IoTCoreClient(mockIotMqttClient);
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
        IoTCoreClient iotCoreClient = new IoTCoreClient(mockIotMqttClient);
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");
        iotCoreClient.updateSubscriptions(topics, message -> {
        });

        reset(mockIotMqttClient);

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
        IoTCoreClient iotCoreClient = new IoTCoreClient(mockIotMqttClient);
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
        IoTCoreClient iotCoreClient = new IoTCoreClient(mockIotMqttClient);
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");
        iotCoreClient.updateSubscriptions(topics, message -> {
        });

        byte[] messageFromLocalMqtt = "message from local mqtt".getBytes();

        iotCoreClient.publish(new Message("mapped/topic/from/local/mqtt", messageFromLocalMqtt));

        ArgumentCaptor<PublishRequest> requestCapture = ArgumentCaptor.forClass(PublishRequest.class);
        verify(mockIotMqttClient, times(1)).publish(requestCapture.capture());

        assertThat(requestCapture.getValue().getTopic(), Matchers.is(Matchers.equalTo("mapped/topic/from/local/mqtt")));
        assertThat(requestCapture.getValue().getPayload(), Matchers.is(Matchers.equalTo(messageFromLocalMqtt)));
    }

    @Test
    void GIVEN_iotcore_client_WHEN_update_subscriptions_with_null_message_handler_THEN_throws() {
        IoTCoreClient iotCoreClient = new IoTCoreClient(mockIotMqttClient);
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");
        assertThrows(NullPointerException.class, () -> iotCoreClient.updateSubscriptions(topics, null));
    }
}
