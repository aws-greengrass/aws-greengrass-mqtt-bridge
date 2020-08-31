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
import java.util.function.Consumer;

public class PubSubClient {
    @Getter(AccessLevel.PACKAGE)
    private Set<String> pubSubTopics = new HashSet<>();

    @Getter(AccessLevel.PACKAGE)
    private Map<String, List<String>> routeMqttToPubSub = new HashMap<>();

    private final PubSubIPCAgent pubSubIPCAgent;

    private final MessageBridge messageBridge;

    private final TopicMapping topicMapping;

    private final MessageBridge.MessageListener mqttListener = (sourceType, msg) -> forwardToPubSub(msg);

    private final Consumer<MessagePublishedEvent> pubSubCallback = this::forwardToMqtt;

    /**
     * Constructor for PubSubClient.
     *
     * @param messageBridge  MessageBridge instance to listen and notify
     * @param topicMapping   topic mapping
     * @param pubSubIPCAgent for interacting with PubSub
     */
    public PubSubClient(MessageBridge messageBridge, TopicMapping topicMapping, PubSubIPCAgent pubSubIPCAgent) {
        this.messageBridge = messageBridge;
        this.topicMapping = topicMapping;
        this.pubSubIPCAgent = pubSubIPCAgent;
    }

    /**
     * set up listener, route maps and subscriptions.
     */
    public void start() {
        messageBridge.addListener(mqttListener, TopicMapping.TopicType.LocalMqtt);
        updateRoutingConfigAndSubscriptions();
    }

    /**
     * tear down listener, route maps and subscriptions.
     */
    public void stop() {
        messageBridge.removeListener(mqttListener, TopicMapping.TopicType.LocalMqtt);
        removeRoutingConfigAndSubscriptions();
    }

    synchronized void updateRoutingConfigAndSubscriptions() {
        routeMqttToPubSub = getRouteMap(TopicMapping.TopicType.LocalMqtt, TopicMapping.TopicType.Pubsub);

        Set<String> prevPubSubTopics = new HashSet<>(pubSubTopics);
        pubSubTopics = getSubscriptionTopics(TopicMapping.TopicType.Pubsub);

        //Subscribe to newly added topics and unsubscribe from removed topics
        Set<String> topicsToSubscribe = new HashSet<>(pubSubTopics);
        topicsToSubscribe.removeAll(prevPubSubTopics);
        for (String pubSubTopic: topicsToSubscribe) {
            subscribeToPubSub(pubSubTopic);
        }
        prevPubSubTopics.removeAll(pubSubTopics);
        for (String pubSubTopic: prevPubSubTopics) {
            unsubscribeFromPubSub(pubSubTopic);
        }
    }

    private synchronized void removeRoutingConfigAndSubscriptions() {
        for (String pubSubTopic: pubSubTopics) {
            unsubscribeFromPubSub(pubSubTopic);
        }
        pubSubTopics.clear();
        routeMqttToPubSub.clear();
    }

    private Set<String> getSubscriptionTopics(TopicMapping.TopicType sourceType) {
        Set<String> subscriptionTopics = new HashSet<>();

        for (TopicMapping.MappingEntry entry: topicMapping.getMapping()) {
            if (entry.getSourceTopicType() == sourceType) {
                subscriptionTopics.add(entry.getSourceTopic());
            }
        }

        return subscriptionTopics;
    }

    private Map<String, List<String>> getRouteMap(TopicMapping.TopicType sourceType, TopicMapping.TopicType destType) {
        Map<String, List<String>> routeMap = new HashMap<>();

        for (TopicMapping.MappingEntry entry: topicMapping.getMapping()) {
            if (entry.getSourceTopicType() == sourceType && entry.getDestTopicType() == destType) {
                routeMap.computeIfAbsent(
                        entry.getSourceTopic(), k -> new ArrayList<>()).add(entry.getDestTopic());
            }
        }

        return routeMap;
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
        pubSubIPCAgent.subscribe(subscribeRequest, pubSubCallback);
    }

    private void unsubscribeFromPubSub(String topic) {
        PubSubUnsubscribeRequest unsubscribeRequest = PubSubUnsubscribeRequest.builder().topic(topic).build();
        pubSubIPCAgent.unsubscribe(unsubscribeRequest, pubSubCallback);
    }

    private void forwardToMqtt(MessagePublishedEvent message) {
        Message forwardMessage = new Message(message.getTopic(), message.getPayload());
        messageBridge.notifyMessage(forwardMessage, TopicMapping.TopicType.Pubsub);
    }
}
