package com.aws.iot.evergreen.mqtt.bridge;

import com.aws.iot.evergreen.builtin.services.pubsub.PubSubIPCAgent;
import com.aws.iot.evergreen.ipc.services.pubsub.MessagePublishedEvent;
import com.aws.iot.evergreen.ipc.services.pubsub.PubSubPublishRequest;
import com.aws.iot.evergreen.ipc.services.pubsub.PubSubSubscribeRequest;
import com.aws.iot.evergreen.ipc.services.pubsub.PubSubUnsubscribeRequest;
import lombok.AccessLevel;
import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PubSubClient {
    @Getter(AccessLevel.PACKAGE)
    private Map<String, List<String>> routePubSubToMqtt = new HashMap<>();

    @Getter(AccessLevel.PACKAGE)
    private Map<String, List<String>> routeMqttToPubSub = new HashMap<>();

    private final PubSubIPCAgent pubSubIPCAgent;

    private final MessageBridge messageBridge;

    /**
     * Constructor for PubSubClient.
     *
     * @param messageBridge  MessageBridge instance to listen and notify
     * @param pubSubIPCAgent for interacting with PubSub
     */
    public PubSubClient(MessageBridge messageBridge, PubSubIPCAgent pubSubIPCAgent) {
        this.messageBridge = messageBridge;
        this.pubSubIPCAgent = pubSubIPCAgent;
        this.messageBridge.addListener((sourceType, msg) -> forwardToPubSub(msg), TopicMapping.TopicType.LocalMqtt);
    }

    /**
     * Update routing maps and PubSub subscriptions.
     *
     * @param topicMapping topic mapping
     */
    public void updateRoutingConfig(TopicMapping topicMapping) {
        Set<String> prevPubSubTopics = new HashSet<>(routePubSubToMqtt.keySet());

        updateRouteMaps(topicMapping);

        //Subscribe to newly added topics and unsubscribe from removed topics
        Set<String> currPubSubTopics = routePubSubToMqtt.keySet();
        Set<String> topicsToSubscribe = new HashSet<>(currPubSubTopics);
        topicsToSubscribe.removeAll(prevPubSubTopics);
        for (String pubSubTopic: topicsToSubscribe) {
            subscribeToPubSub(pubSubTopic);
        }
        prevPubSubTopics.removeAll(currPubSubTopics);
        for (String pubSubTopic: prevPubSubTopics) {
            unsubscribeFromPubSub(pubSubTopic);
        }
    }

    private void updateRouteMaps(TopicMapping topicMapping) {
        //TODO: Construct route maps differentially
        routePubSubToMqtt = new HashMap<>();
        routeMqttToPubSub = new HashMap<>();

        for (TopicMapping.MappingEntry entry: topicMapping.getMapping()) {
            switch (entry.getSourceTopicType()) {
                case LocalMqtt:
                    if (entry.getDestTopicType() == TopicMapping.TopicType.Pubsub) {
                        routeMqttToPubSub.computeIfAbsent(
                                entry.getSourceTopic(), k -> new ArrayList<>()).add(entry.getDestTopic());
                    }
                    break;

                case Pubsub:
                    routePubSubToMqtt.computeIfAbsent(
                            entry.getSourceTopic(), k -> new ArrayList<>()).add(entry.getDestTopic());
                    break;

                default:
                    break;
            }
        }
    }

    private void forwardToPubSub(Message message) {
        String sourceTopic = message.getTopic();
        if (routeMqttToPubSub.containsKey(sourceTopic)) { //ignores messages meant for IoTCore
            for (String destTopic: routeMqttToPubSub.get(sourceTopic)) {
                publishToPubSub(destTopic, message.getPayload());
            }
        }
    }

    private void publishToPubSub(String topic, byte[] payload) {
        PubSubPublishRequest publishRequest = PubSubPublishRequest.builder().topic(topic).payload(payload).build();
        pubSubIPCAgent.publish(publishRequest);
    }

    private void subscribeToPubSub(String topic) {
        PubSubSubscribeRequest subscribeRequest = PubSubSubscribeRequest.builder().topic(topic).build();
        pubSubIPCAgent.subscribe(subscribeRequest, this::forwardToMqtt);
    }

    private void unsubscribeFromPubSub(String topic) {
        PubSubUnsubscribeRequest unsubscribeRequest = PubSubUnsubscribeRequest.builder().topic(topic).build();
        pubSubIPCAgent.unsubscribe(unsubscribeRequest, this::forwardToMqtt);
    }

    private void forwardToMqtt(MessagePublishedEvent message) {
        String sourceTopic = message.getTopic();
        for (String destTopic: routePubSubToMqtt.get(sourceTopic)) {
            messageBridge.notifyMessage(new Message(destTopic, message.getPayload()), TopicMapping.TopicType.Pubsub);
        }
    }
}
