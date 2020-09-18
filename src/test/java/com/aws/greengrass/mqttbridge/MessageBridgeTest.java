/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge;

import com.aws.greengrass.mqttbridge.clients.MessageClient;
import com.aws.greengrass.mqttbridge.clients.MessageClientException;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Set;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class MessageBridgeTest {
    @Mock
    private MessageClient mockMessageClient;
    @Mock
    private MessageClient mockMessageClient2;
    @Mock
    private MessageClient mockMessageClient3;

    @Mock
    private TopicMapping mockTopicMapping;

    @Test
    void WHEN_call_message_bridge_constructer_THEN_does_not_throw() {
        new MessageBridge(mockTopicMapping);
        verify(mockTopicMapping, times(1)).listenToUpdates(any());
    }

    @Test
    void GIVEN_mqtt_bridge_and_mapping_populated_WHEN_add_client_THEN_subscribed() throws Exception {
        TopicMapping mapping = new TopicMapping();
        mapping.updateMapping("[\n"
                + "  {\"SourceTopic\": \"mqtt/topic\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/cloud/topic\", \"DestTopicType\": \"IotCore\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic2\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/pubsub/topic\", \"DestTopicType\": \"Pubsub\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic3\", \"SourceTopicType\": \"IotCore\", \"DestTopic\": "
                + "\"/test/cloud/topic2\", \"DestTopicType\": \"LocalMqtt\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic4\", \"SourceTopicType\": \"Pubsub\", \"DestTopic\": "
                + "\"/test/cloud/topic2\", \"DestTopicType\": \"LocalMqtt\"}\n" + "]");
        MessageBridge messageBridge = new MessageBridge(mapping);
        messageBridge.addOrReplaceMessageClient(TopicMapping.TopicType.LocalMqtt, mockMessageClient);
        ArgumentCaptor<Set<String>> topicsArgumentCaptor = ArgumentCaptor.forClass(Set.class);
        verify(mockMessageClient, times(1)).updateSubscriptions(topicsArgumentCaptor.capture(), any());
        MatcherAssert.assertThat(topicsArgumentCaptor.getValue(), Matchers.hasSize(2));
        MatcherAssert
                .assertThat(topicsArgumentCaptor.getValue(), Matchers.containsInAnyOrder("mqtt/topic", "mqtt/topic2"));

        reset(mockMessageClient);
        messageBridge.addOrReplaceMessageClient(TopicMapping.TopicType.Pubsub, mockMessageClient);
        topicsArgumentCaptor = ArgumentCaptor.forClass(Set.class);
        verify(mockMessageClient, times(1)).updateSubscriptions(topicsArgumentCaptor.capture(), any());
        MatcherAssert.assertThat(topicsArgumentCaptor.getValue(), Matchers.hasSize(1));
        MatcherAssert.assertThat(topicsArgumentCaptor.getValue(), Matchers.containsInAnyOrder("mqtt/topic4"));

        reset(mockMessageClient);
        messageBridge.addOrReplaceMessageClient(TopicMapping.TopicType.IotCore, mockMessageClient);
        topicsArgumentCaptor = ArgumentCaptor.forClass(Set.class);
        verify(mockMessageClient, times(1)).updateSubscriptions(topicsArgumentCaptor.capture(), any());
        MatcherAssert.assertThat(topicsArgumentCaptor.getValue(), Matchers.hasSize(1));
        MatcherAssert.assertThat(topicsArgumentCaptor.getValue(), Matchers.containsInAnyOrder("mqtt/topic3"));
    }

    @Test
    void GIVEN_mqtt_bridge_and_clients_WHEN_mapping_populated_THEN_subscribed() throws Exception {
        TopicMapping mapping = new TopicMapping();
        MessageBridge messageBridge = new MessageBridge(mapping);

        messageBridge.addOrReplaceMessageClient(TopicMapping.TopicType.LocalMqtt, mockMessageClient);
        messageBridge.addOrReplaceMessageClient(TopicMapping.TopicType.Pubsub, mockMessageClient2);
        messageBridge.addOrReplaceMessageClient(TopicMapping.TopicType.IotCore, mockMessageClient3);

        reset(mockMessageClient);
        reset(mockMessageClient2);
        reset(mockMessageClient3);

        mapping.updateMapping("[\n"
                + "  {\"SourceTopic\": \"mqtt/topic\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/cloud/topic\", \"DestTopicType\": \"IotCore\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic2\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/pubsub/topic\", \"DestTopicType\": \"Pubsub\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic3\", \"SourceTopicType\": \"IotCore\", \"DestTopic\": "
                + "\"/test/cloud/topic2\", \"DestTopicType\": \"LocalMqtt\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic4\", \"SourceTopicType\": \"Pubsub\", \"DestTopic\": "
                + "\"/test/cloud/topic2\", \"DestTopicType\": \"LocalMqtt\"}\n" + "]");

        ArgumentCaptor<Set<String>> topicsArgumentCaptorLocalMqtt = ArgumentCaptor.forClass(Set.class);
        verify(mockMessageClient, times(1)).updateSubscriptions(topicsArgumentCaptorLocalMqtt.capture(), any());
        MatcherAssert.assertThat(topicsArgumentCaptorLocalMqtt.getValue(), Matchers.hasSize(2));
        MatcherAssert.assertThat(topicsArgumentCaptorLocalMqtt.getValue(),
                Matchers.containsInAnyOrder("mqtt/topic", "mqtt/topic2"));

        ArgumentCaptor<Set<String>> topicsArgumentCaptorLocalPubsub = ArgumentCaptor.forClass(Set.class);
        verify(mockMessageClient2, times(1)).updateSubscriptions(topicsArgumentCaptorLocalPubsub.capture(), any());
        MatcherAssert.assertThat(topicsArgumentCaptorLocalPubsub.getValue(), Matchers.hasSize(1));
        MatcherAssert
                .assertThat(topicsArgumentCaptorLocalPubsub.getValue(), Matchers.containsInAnyOrder("mqtt/topic4"));

        ArgumentCaptor<Set<String>> topicsArgumentCaptorIotCore = ArgumentCaptor.forClass(Set.class);
        verify(mockMessageClient3, times(1)).updateSubscriptions(topicsArgumentCaptorIotCore.capture(), any());
        MatcherAssert.assertThat(topicsArgumentCaptorIotCore.getValue(), Matchers.hasSize(1));
        MatcherAssert.assertThat(topicsArgumentCaptorIotCore.getValue(), Matchers.containsInAnyOrder("mqtt/topic3"));
    }

    @Test
    void GIVEN_mqtt_bridge_and_client_WHEN_client_removed_THEN_no_subscriptions_made() throws Exception {
        TopicMapping mapping = new TopicMapping();
        MessageBridge messageBridge = new MessageBridge(mapping);

        messageBridge.addOrReplaceMessageClient(TopicMapping.TopicType.LocalMqtt, mockMessageClient);
        messageBridge.removeMessageClient(TopicMapping.TopicType.LocalMqtt);

        reset(mockMessageClient);
        mapping.updateMapping("[\n"
                + "  {\"SourceTopic\": \"mqtt/topic\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/cloud/topic\", \"DestTopicType\": \"IotCore\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic2\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/pubsub/topic\", \"DestTopicType\": \"Pubsub\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic3\", \"SourceTopicType\": \"IotCore\", \"DestTopic\": "
                + "\"/test/cloud/topic2\", \"DestTopicType\": \"LocalMqtt\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic4\", \"SourceTopicType\": \"Pubsub\", \"DestTopic\": "
                + "\"/test/cloud/topic2\", \"DestTopicType\": \"LocalMqtt\"}\n" + "]");

        ArgumentCaptor<Set<String>> topicsArgumentCaptorLocalMqtt = ArgumentCaptor.forClass(Set.class);
        verify(mockMessageClient, times(0)).updateSubscriptions(topicsArgumentCaptorLocalMqtt.capture(), any());
    }

    @Test
    void GIVEN_mqtt_bridge_with_mapping_WHEN_mapping_updated_THEN_subscriptions_updated() throws Exception {
        TopicMapping mapping = new TopicMapping();
        MessageBridge messageBridge = new MessageBridge(mapping);

        messageBridge.addOrReplaceMessageClient(TopicMapping.TopicType.LocalMqtt, mockMessageClient);
        messageBridge.addOrReplaceMessageClient(TopicMapping.TopicType.Pubsub, mockMessageClient2);
        messageBridge.addOrReplaceMessageClient(TopicMapping.TopicType.IotCore, mockMessageClient3);
        mapping.updateMapping("[\n"
                + "  {\"SourceTopic\": \"mqtt/topic\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/cloud/topic\", \"DestTopicType\": \"IotCore\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic2\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/pubsub/topic\", \"DestTopicType\": \"Pubsub\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic3\", \"SourceTopicType\": \"IotCore\", \"DestTopic\": "
                + "\"/test/cloud/topic2\", \"DestTopicType\": \"LocalMqtt\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic4\", \"SourceTopicType\": \"Pubsub\", \"DestTopic\": "
                + "\"/test/cloud/topic2\", \"DestTopicType\": \"LocalMqtt\"}\n" + "]");

        reset(mockMessageClient);
        reset(mockMessageClient2);
        reset(mockMessageClient3);

        // Change topic 2
        // Add a new topic 3
        // Modify old topic 3 to come from Pubsub
        // Remove topic 4
        mapping.updateMapping(
                "[\n" + "  {\"SourceTopic\": \"mqtt/topic\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": "
                        + "\"/test/cloud/topic\", \"DestTopicType\": \"IotCore\"},\n"
                        + "  {\"SourceTopic\": \"mqtt/topic2/changed\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": "
                        + "\"/test/pubsub/topic/changed\", \"DestTopicType\": \"Pubsub\"},\n"
                        + "  {\"SourceTopic\": \"mqtt/topic3/added\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": "
                        + "\"/test/pubsub/topic/added\", \"DestTopicType\": \"Pubsub\"},\n"
                        + "  {\"SourceTopic\": \"mqtt/topic3\", \"SourceTopicType\": \"Pubsub\", \"DestTopic\": "
                        + "\"/test/pubsub/topic2\", \"DestTopicType\": \"LocalMqtt\"}\n" + "]");

        ArgumentCaptor<Set<String>> topicsArgumentCaptorLocalMqtt = ArgumentCaptor.forClass(Set.class);
        verify(mockMessageClient, times(1)).updateSubscriptions(topicsArgumentCaptorLocalMqtt.capture(), any());
        MatcherAssert.assertThat(topicsArgumentCaptorLocalMqtt.getValue(), Matchers.hasSize(3));
        MatcherAssert.assertThat(topicsArgumentCaptorLocalMqtt.getValue(),
                Matchers.containsInAnyOrder("mqtt/topic", "mqtt" + "/topic2/changed", "mqtt/topic3/added"));

        ArgumentCaptor<Set<String>> topicsArgumentCaptorLocalPubsub = ArgumentCaptor.forClass(Set.class);
        verify(mockMessageClient2, times(1)).updateSubscriptions(topicsArgumentCaptorLocalPubsub.capture(), any());
        MatcherAssert.assertThat(topicsArgumentCaptorLocalPubsub.getValue(), Matchers.hasSize(1));
        MatcherAssert
                .assertThat(topicsArgumentCaptorLocalPubsub.getValue(), Matchers.containsInAnyOrder("mqtt/topic3"));

        ArgumentCaptor<Set<String>> topicsArgumentCaptorIotCore = ArgumentCaptor.forClass(Set.class);
        verify(mockMessageClient3, times(1)).updateSubscriptions(topicsArgumentCaptorIotCore.capture(), any());
        MatcherAssert.assertThat(topicsArgumentCaptorIotCore.getValue(), Matchers.hasSize(0));
    }

    @Test
    void GIVEN_mqtt_bridge_and_mapping_populated_WHEN_receive_mqtt_message_THEN_routed_to_iotcore_pubsub()
            throws Exception {
        TopicMapping mapping = new TopicMapping();
        mapping.updateMapping("[\n"
                + "  {\"SourceTopic\": \"mqtt/topic\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/cloud/topic\", \"DestTopicType\": \"IotCore\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": "
                + "\"/test/pubsub/topic\", \"DestTopicType\": \"Pubsub\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic2\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/pubsub/topic\", \"DestTopicType\": \"Pubsub\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic2\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": "
                + "\"/test/pubsub/topic2\", \"DestTopicType\": \"Pubsub\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic3\", \"SourceTopicType\": \"IotCore\", \"DestTopic\": "
                + "\"/test/cloud/topic2\", \"DestTopicType\": \"LocalMqtt\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic4\", \"SourceTopicType\": \"Pubsub\", \"DestTopic\": "
                + "\"/test/cloud/topic2\", \"DestTopicType\": \"LocalMqtt\"}\n" + "]");

        MessageBridge messageBridge = new MessageBridge(mapping);

        messageBridge.addOrReplaceMessageClient(TopicMapping.TopicType.LocalMqtt, mockMessageClient);
        messageBridge.addOrReplaceMessageClient(TopicMapping.TopicType.Pubsub, mockMessageClient2);
        messageBridge.addOrReplaceMessageClient(TopicMapping.TopicType.IotCore, mockMessageClient3);

        ArgumentCaptor<Consumer> messageHandlerLocalMqttCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(mockMessageClient, times(1)).updateSubscriptions(any(), messageHandlerLocalMqttCaptor.capture());
        ArgumentCaptor<Consumer> messageHandlerPubSubCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(mockMessageClient2, times(1)).updateSubscriptions(any(), messageHandlerPubSubCaptor.capture());
        ArgumentCaptor<Consumer> messageHandlerIotCoreCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(mockMessageClient3, times(1)).updateSubscriptions(any(), messageHandlerIotCoreCaptor.capture());

        //Make mockMessageClient3 throw. (Will be ignored)
        doThrow(new MessageClientException("")).when(mockMessageClient3).publish(any());

        byte[] messageOnTopic1 = "message from topic mqtt/topic".getBytes();
        byte[] messageOnTopic2 = "message from topic mqtt/topic2".getBytes();
        messageHandlerLocalMqttCaptor.getValue().accept(new Message("mqtt/topic", messageOnTopic1));
        messageHandlerLocalMqttCaptor.getValue().accept(new Message("mqtt/topic2", messageOnTopic2));

        // Also send on an unknown topic
        messageHandlerLocalMqttCaptor.getValue().accept(new Message("mqtt/unknown", messageOnTopic2));

        verify(mockMessageClient, times(0)).publish(any());
        ArgumentCaptor<Message> messagePubSubCaptor = ArgumentCaptor.forClass(Message.class);
        verify(mockMessageClient2, times(3)).publish(messagePubSubCaptor.capture());
        ArgumentCaptor<Message> messageIotCoreCaptor = ArgumentCaptor.forClass(Message.class);
        verify(mockMessageClient3, times(1)).publish(messageIotCoreCaptor.capture());

        MatcherAssert.assertThat(messageIotCoreCaptor.getAllValues().get(0).getTopic(),
                Matchers.is(Matchers.equalTo("/test/cloud/topic")));
        Assertions.assertArrayEquals(messageOnTopic1, messageIotCoreCaptor.getAllValues().get(0).getPayload());

        MatcherAssert.assertThat(messagePubSubCaptor.getAllValues().get(0).getTopic(),
                Matchers.is(Matchers.equalTo("/test/pubsub/topic")));
        Assertions.assertArrayEquals(messageOnTopic1, messagePubSubCaptor.getAllValues().get(0).getPayload());
        MatcherAssert.assertThat(messagePubSubCaptor.getAllValues().get(1).getTopic(),
                Matchers.is(Matchers.equalTo("/test/pubsub/topic")));
        Assertions.assertArrayEquals(messageOnTopic2, messagePubSubCaptor.getAllValues().get(1).getPayload());
        MatcherAssert.assertThat(messagePubSubCaptor.getAllValues().get(2).getTopic(),
                Matchers.is(Matchers.equalTo("/test/pubsub/topic2")));
        Assertions.assertArrayEquals(messageOnTopic2, messagePubSubCaptor.getAllValues().get(2).getPayload());
    }
}
