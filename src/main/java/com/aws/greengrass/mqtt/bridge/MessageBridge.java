/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqtt.bridge.clients.MessageClient;
import com.aws.greengrass.mqtt.bridge.clients.MessageClientException;
import com.aws.greengrass.mqtt.bridge.model.Message;
import com.aws.greengrass.mqtt.bridge.model.Mqtt5RouteOptions;
import com.aws.greengrass.mqtt.bridge.model.MqttMessage;
import com.aws.greengrass.mqttclient.MqttTopic;
import com.aws.greengrass.util.Utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

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
    // Read on every message, so route option changes apply without reinstalling the bridge
    private final Supplier<Map<String, Mqtt5RouteOptions>> mqtt5RouteOptionsByRoute;
    // A map from type of message client to the clients. For example, LocalMqtt -> MQTTClient
    private final Map<TopicMapping.TopicType, MessageClient<? extends Message>> messageClientMap
            = new ConcurrentHashMap<>();
    // A map from type of source to its mapping. The mapping is actually mapping from topic name to its destinations
    // (route name -> destination topic + type). This data structure may change once we introduce complex routing
    // mechanism.
    // Example:
    // LocalMqtt -> {"/sourceTopic", {"route1": <TopicMapping>, "route2": <TopicMapping>}}
    private Map<TopicMapping.TopicType, Map<String, Map<String, TopicMapping.MappingEntry>>>
            perClientSourceDestinationMap = new HashMap<>();

    /**
     * Ctr for Message Bridge.
     *
     * @param topicMapping              topics mapping
     * @param mqtt5RouteOptionsByRoute  mqtt5 route options, keyed by route name
     */
    public MessageBridge(TopicMapping topicMapping, Map<String, Mqtt5RouteOptions> mqtt5RouteOptionsByRoute) {
        this(topicMapping, () -> mqtt5RouteOptionsByRoute);
    }

    /**
     * Ctr for Message Bridge.
     *
     * @param topicMapping              topics mapping
     * @param mqtt5RouteOptionsByRoute  supplier of the current mqtt5 route options, keyed by route name
     */
    public MessageBridge(TopicMapping topicMapping,
                         Supplier<Map<String, Mqtt5RouteOptions>> mqtt5RouteOptionsByRoute) {
        this.topicMapping = topicMapping;
        this.topicMapping.listenToUpdates(this::processMappingAndSubscribe);
        this.mqtt5RouteOptionsByRoute = mqtt5RouteOptionsByRoute;

        processMappingAndSubscribe();
    }

    /**
     * Add or replace the client of given type and update subscriptions for the client.
     *
     * @param clientType    type of the client (type is the `source` type). Example, it will be LocalMqtt for
     *                      MQTTClient
     * @param messageClient client
     */
    public void addOrReplaceMessageClientAndUpdateSubscriptions(
            TopicMapping.TopicType clientType, MessageClient<? extends Message> messageClient) {
        messageClientMap.put(clientType, messageClient);
        updateSubscriptionsForClient(clientType, messageClient);
    }

    /**
     * Remove the client of given type.
     *
     * @param clientType client type
     */
    public void removeMessageClient(TopicMapping.TopicType clientType) {
        messageClientMap.remove(clientType);
    }

    private <T extends Message> void handleMessage(TopicMapping.TopicType sourceType, T message) {
        String fullSourceTopic = message.getTopic();
        LOGGER.atDebug().kv(LOG_KEY_SOURCE_TYPE, sourceType).kv(LOG_KEY_SOURCE_TOPIC, fullSourceTopic)
                .log("Message received");

        MessageClient<? extends Message> sourceClient = messageClientMap.get(sourceType);
        if (sourceClient == null) {
            LOGGER.atError().kv(LOG_KEY_SOURCE_TYPE, sourceType).kv(LOG_KEY_SOURCE_TOPIC, fullSourceTopic)
                    .log("Source client not found");
            return;
        }

        Map<String, Map<String, TopicMapping.MappingEntry>> srcDestMapping =
                perClientSourceDestinationMap.get(sourceType);

        if (srcDestMapping != null) {
            final BiConsumer<String, TopicMapping.MappingEntry> processDestination = (route, mapping) -> {
                MessageClient<? extends Message> client = messageClientMap.get(mapping.getTarget());
                // If the mapped topic string is empty string, we forward the message to the same topic as the
                // source topic.
                final String baseTargetTopic = Utils.isEmpty(mapping.getTargetTopic())
                    ? fullSourceTopic
                    : mapping.getTargetTopic();
                final String targetTopic = Utils.isEmpty(mapping.getTargetTopicPrefix())
                    ? baseTargetTopic
                    : mapping.getTargetTopicPrefix() + baseTargetTopic;
                if (client == null) {
                    LOGGER.atError().kv(LOG_KEY_SOURCE_TYPE, sourceType).kv(LOG_KEY_SOURCE_TOPIC, fullSourceTopic)
                            .kv(LOG_KEY_TARGET_TYPE, mapping.getTarget())
                            .kv(LOG_KEY_TARGET_TOPIC, mapping.getTargetTopic())
                            .kv(LOG_KEY_RESOLVED_TARGET_TOPIC, targetTopic)
                            .log("Message client not found for target type");
                } else {
                    try {
                        publishMessage(client, targetTopic, message, isRetainAsPublished(route, mapping));
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
                }
            };

            srcDestMapping.entrySet().stream()
                    .filter(mapping -> MqttTopic.topicIsSupersetOf(mapping.getKey(), fullSourceTopic))
                    .map(Map.Entry::getValue)
                    .forEach(perTopicDestinations -> perTopicDestinations.forEach(processDestination));
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends Message> void publishMessage(MessageClient<T> client, String topic, Message message,
                                                    boolean retainAsPublished) throws MessageClientException {
        T msg = client.convertMessage(message);

        if (!retainAsPublished && msg instanceof MqttMessage) {
            MqttMessage mqttMsg = (MqttMessage) msg;
            mqttMsg = mqttMsg.toBuilder().topic(topic).retain(false).build();
            client.publish((T) mqttMsg);
        } else {
            msg = (T) msg.newFromMessageWithTopic(topic);
            client.publish(msg);
        }
    }

    private boolean isRetainAsPublished(String route, TopicMapping.MappingEntry mapping) {
        // retainAsPublished is only supported for routes where the source is LocalMqtt
        if (mapping.getSource() != TopicMapping.TopicType.LocalMqtt) {
            return DEFAULT_RETAIN_AS_PUBLISHED;
        }
        return Optional.ofNullable(mqtt5RouteOptionsByRoute.get().get(route))
                .map(Mqtt5RouteOptions::isRetainAsPublished)
                .orElse(DEFAULT_RETAIN_AS_PUBLISHED);
    }

    private void processMappingAndSubscribe() {
        Map<String, TopicMapping.MappingEntry> mapping = topicMapping.getMapping();
        LOGGER.atDebug().kv("topicMapping", mapping).log("Processing mapping");

        Map<TopicMapping.TopicType, Map<String, Map<String, TopicMapping.MappingEntry>>>
                perClientSourceDestinationMapTemp = new HashMap<>();

        mapping.forEach((key, mappingEntry) -> {
            // Ensure mapping for client type
            Map<String, Map<String, TopicMapping.MappingEntry>> sourceDestinationMap =
                    perClientSourceDestinationMapTemp.computeIfAbsent(mappingEntry.getSource(), k -> new HashMap<>());

            // Add destinations for each source topic
            // TODO: Support more types of topic mapping.
            sourceDestinationMap.computeIfAbsent(mappingEntry.getTopic(), k -> new HashMap<>())
                    .put(key, mappingEntry);
        });

        perClientSourceDestinationMap = perClientSourceDestinationMapTemp;

        messageClientMap.forEach(this::updateSubscriptionsForClient);
        LOGGER.atDebug().kv("topicMapping", perClientSourceDestinationMap).log("Processed mapping");
    }

    private synchronized void updateSubscriptionsForClient(TopicMapping.TopicType clientType,
                                                           MessageClient<? extends Message> messageClient) {
        Map<String, Map<String, TopicMapping.MappingEntry>> srcDestMapping =
                perClientSourceDestinationMap.get(clientType);

        Set<String> topicsToSubscribe;
        if (srcDestMapping == null) {
            topicsToSubscribe = new HashSet<>();
        } else {
            topicsToSubscribe = srcDestMapping.keySet();
        }

        LOGGER.atDebug().kv("clientType", clientType).kv("topics", topicsToSubscribe).log("Updating subscriptions");

        messageClient.updateSubscriptions(topicsToSubscribe, message -> handleMessage(clientType, message));
    }
}
