/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqtt.bridge.clients.MessageClient;
import com.aws.greengrass.mqtt.bridge.clients.MessageClientException;
import com.aws.greengrass.mqtt.bridge.clients.MessageClients;
import com.aws.greengrass.mqtt.bridge.model.BridgeConfigReference;
import com.aws.greengrass.mqtt.bridge.model.Message;
import com.aws.greengrass.mqtt.bridge.model.Mqtt5RouteOptions;
import com.aws.greengrass.mqtt.bridge.model.MqttMessage;
import com.aws.greengrass.util.Utils;
import org.eclipse.paho.client.mqttv3.MqttTopic;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static com.aws.greengrass.mqtt.bridge.model.Mqtt5RouteOptions.DEFAULT_RETAIN_AS_PUBLISHED;


/**
 * Bridges/Routes the messages flowing between clients to various brokers. This class process the topics mappings. It
 * tells the clients to subscribe to the relevant topics and routes the messages to other clients when received.
 */
public class MessageBridge {
    private static final Logger LOGGER = LogManager.getLogger(MessageBridge.class);
    private static final String LOG_KEY_SOURCE_TYPE = "source";
    private static final String LOG_KEY_SOURCE_TOPIC = "sourceTopic";
    private static final String LOG_KEY_TARGET_TYPE = "target";
    private static final String LOG_KEY_TARGET_TOPIC = "targetTopic";
    private static final String LOG_KEY_RESOLVED_TARGET_TOPIC = "resolvedTargetTopic";

    private final TopicMapping topicMapping;
    private final BridgeConfigReference bridgeConfig;
    // A map from type of message client to the clients. For example, LocalMqtt -> MQTTClient
    private final MessageClients messageClients;

    private Map<String, Mqtt5RouteOptions> localMqttOptionsByTopic;

    // A map from type of source to its mapping. The mapping is actually mapping from topic name to its destinations
    // (destination topic + type). This data structure may change once we introduce complex routing mechanism.
    // Example:
    // LocalMqtt -> {"/sourceTopic", [<TopicMapping>, <TopicMapping>]}
    private Map<TopicMapping.TopicType, Map<String, List<TopicMapping.MappingEntry>>>
            perClientSourceDestinationMap = new HashMap<>();

    /**
     * Ctr for Message Bridge.
     *
     * @param topicMapping   topics mapping
     * @param bridgeConfig   bridge config
     * @param messageClients message clients
     */
    @Inject
    public MessageBridge(TopicMapping topicMapping,
                         BridgeConfigReference bridgeConfig,
                         MessageClients messageClients) {
        this.topicMapping = topicMapping;
        this.bridgeConfig = bridgeConfig;
        this.messageClients = messageClients;
    }

    public void initialize() throws MessageClientException {
        this.topicMapping.listenToUpdates(this::processMappingAndSubscribe);
        this.localMqttOptionsByTopic = bridgeConfig.get()
                .getMqtt5RouteOptionsForSource(TopicMapping.TopicType.LocalMqtt);
        processMappingAndSubscribe();
    }

    private <T extends Message> void handleMessage(MessageClient<? extends Message> sourceMessageClient, T message) {
        String fullSourceTopic = message.getTopic();
        LOGGER.atDebug()
                .kv(LOG_KEY_SOURCE_TYPE, sourceMessageClient.getClass().getSimpleName())
                .kv(LOG_KEY_SOURCE_TOPIC, fullSourceTopic)
                .log("Message received");

        TopicMapping.TopicType sourceType = sourceMessageClient.getType();
        Map<String, List<TopicMapping.MappingEntry>> srcDestMapping = perClientSourceDestinationMap.get(sourceType);
        if (srcDestMapping == null) {
            return;
        }

        final Consumer<TopicMapping.MappingEntry> processDestination = mapping -> {
            // If the mapped topic string is empty string, we forward the message to the same topic as the
            // source topic.
            final String baseTargetTopic = Utils.isEmpty(mapping.getTargetTopic())
                    ? fullSourceTopic
                    : mapping.getTargetTopic();
            final String targetTopic = Utils.isEmpty(mapping.getTargetTopicPrefix())
                    ? baseTargetTopic
                    : mapping.getTargetTopicPrefix() + baseTargetTopic;
            try {
                MessageClient<? extends Message> targetMessageClient = messageClients.getMessageClients().stream()
                        .filter(c -> Objects.equals(mapping.getTarget(), c.getType()))
                        .findFirst()
                        .orElseThrow(() ->
                                new MessageClientException("No message client found of type " + mapping.getTarget()));
                publishMessage(targetMessageClient, targetTopic, message);
                LOGGER.atDebug().kv(LOG_KEY_SOURCE_TYPE, sourceType).kv(LOG_KEY_SOURCE_TOPIC, fullSourceTopic)
                        .kv(LOG_KEY_TARGET_TYPE, mapping.getTarget())
                        .kv(LOG_KEY_TARGET_TOPIC, mapping.getTargetTopic())
                        .kv(LOG_KEY_RESOLVED_TARGET_TOPIC, targetTopic).log("Published message");
            } catch (MessageClientException e) {
                LOGGER.atError().kv(LOG_KEY_SOURCE_TYPE, sourceType).kv(LOG_KEY_SOURCE_TOPIC, fullSourceTopic)
                        .kv(LOG_KEY_TARGET_TYPE, mapping.getTarget())
                        .kv(LOG_KEY_TARGET_TOPIC, mapping.getTargetTopic())
                        .kv(LOG_KEY_RESOLVED_TARGET_TOPIC, targetTopic).log("Failed to publish");
            }
        };

        // Do topic filter matching
        srcDestMapping.entrySet().stream()
                .filter(mapping -> MqttTopic.isMatched(mapping.getKey(), fullSourceTopic))
                .map(Map.Entry::getValue)
                .forEach(perTopicDestinationList -> perTopicDestinationList.forEach(processDestination));
    }

    @SuppressWarnings("unchecked")
    private <T extends Message> void publishMessage(MessageClient<T> client, String topic, Message message)
            throws MessageClientException {
        T msg = client.convertMessage(message);

        if (!isRetainAsPublished(topic) && msg instanceof MqttMessage) {
            MqttMessage mqttMsg = (MqttMessage) msg;
            mqttMsg = mqttMsg.toBuilder().topic(topic).retain(false).build();
            client.publish((T) mqttMsg);
        } else {
            msg = (T) msg.newFromMessageWithTopic(topic);
            client.publish(msg);
        }
    }

    private boolean isRetainAsPublished(String topic) {
        return Optional.ofNullable(localMqttOptionsByTopic.get(topic))
                .map(Mqtt5RouteOptions::isRetainAsPublished)
                .orElse(DEFAULT_RETAIN_AS_PUBLISHED);
    }

    private void processMappingAndSubscribe() {
        Map<String, TopicMapping.MappingEntry> mapping = topicMapping.getMapping();
        LOGGER.atDebug().kv("topicMapping", mapping).log("Processing mapping");

        Map<TopicMapping.TopicType, Map<String, List<TopicMapping.MappingEntry>>>
                perClientSourceDestinationMapTemp = new HashMap<>();

        mapping.forEach((key, mappingEntry) -> {
            // Ensure mapping for client type
            Map<String, List<TopicMapping.MappingEntry>> sourceDestinationMap =
                    perClientSourceDestinationMapTemp.computeIfAbsent(mappingEntry.getSource(), k -> new HashMap<>());

            // Add destinations for each source topic
            // TODO: Support more types of topic mapping.
            sourceDestinationMap.computeIfAbsent(mappingEntry.getTopic(), k -> new ArrayList<>())
                    .add(mappingEntry);
        });

        perClientSourceDestinationMap = perClientSourceDestinationMapTemp;

        messageClients.forEach(this::updateSubscriptionsForClient);
        LOGGER.atDebug().kv("topicMapping", perClientSourceDestinationMap).log("Processed mapping");
    }

    private synchronized void updateSubscriptionsForClient(MessageClient<? extends Message> messageClient) {
        Map<String, List<TopicMapping.MappingEntry>> srcDestMapping =
                perClientSourceDestinationMap.get(messageClient.getType());

        Set<String> topicsToSubscribe = srcDestMapping == null ? new HashSet<>() : srcDestMapping.keySet();

        LOGGER.atDebug()
                .kv("clientType", messageClient.getType())
                .kv("topics", topicsToSubscribe)
                .log("Updating subscriptions");

        messageClient.updateSubscriptions(topicsToSubscribe, message -> handleMessage(messageClient, message));
    }
}
