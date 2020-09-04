/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.iot.evergreen.mqtt.bridge;

import com.aws.iot.evergreen.logging.api.Logger;
import com.aws.iot.evergreen.logging.impl.LogManager;
import com.aws.iot.evergreen.mqtt.bridge.clients.MessageClient;
import com.aws.iot.evergreen.mqtt.bridge.clients.MessageClientException;
import com.aws.iot.evergreen.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Bridges/Routes the messages flowing between clients to various brokers. This class process the topics mappings. It
 * tells the clients to subscribe to the relevant topics and routes the messages to other clients when received.
 */
public class MessageBridge {
    private static final Logger LOGGER = LogManager.getLogger(MessageBridge.class);

    private final TopicMapping topicMapping;
    // A map from type of message client to the clients. For example, LocalMqtt -> MQTTClient
    private final Map<TopicMapping.TopicType, MessageClient> messageClientMap = new ConcurrentHashMap<>();
    // A map from type of source to its mapping. The mapping is actually mapping from topic name to its destinations
    // (destination topic + type). This data structure may change once we introduce complex routing mechanism.
    // Example:
    // LocalMqtt -> {"/sourceTopic", [{"/destinationTopic", IoTCore}, {"/destinationTopic2", Pubsub}]}
    private Map<TopicMapping.TopicType, Map<String, List<Pair<String, TopicMapping.TopicType>>>>
            perClientSourceDestinationMap = new HashMap<>();

    /**
     * Ctr for Message Bridge.
     *
     * @param topicMapping topics mapping
     */
    public MessageBridge(TopicMapping topicMapping) {
        this.topicMapping = topicMapping;
        this.topicMapping.listenToUpdates(this::processMappingAndSubscribe);
        processMappingAndSubscribe();
    }

    /**
     * Add or replace the client of given type.
     *
     * @param clientType    type of the client (type is the `source` type). Example, it will be LocalMqtt for
     *                      MQTTClient
     * @param messageClient client
     */
    public void addOrReplaceMessageClient(TopicMapping.TopicType clientType, MessageClient messageClient) {
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

    private void handleMessage(TopicMapping.TopicType sourceType, Message message) {
        String sourceTopic = message.getTopic();
        LOGGER.atDebug().kv("sourceType", sourceType).kv("sourceTopic", sourceTopic).log("Message received");

        Map<String, List<Pair<String, TopicMapping.TopicType>>> srcDestMapping =
                perClientSourceDestinationMap.get(sourceType);
        if (srcDestMapping != null) {
            List<Pair<String, TopicMapping.TopicType>> destinations = srcDestMapping.get(sourceTopic);
            if (destinations == null) {
                return;
            }
            for (Pair<String, TopicMapping.TopicType> destination : destinations) {
                MessageClient client = messageClientMap.get(destination.getRight());
                if (client != null) {
                    Message msg = new Message(destination.getLeft(), message.getPayload());
                    try {
                        client.publish(msg);
                        LOGGER.atDebug().kv("sourceType", sourceType).kv("sourceTopic", sourceTopic)
                                .kv("destType", destination.getRight()).kv("destTopic", destination.getLeft())
                                .log("Published message");
                    } catch (MessageClientException e) {
                        LOGGER.atError().kv("sourceType", sourceType).kv("sourceTopic", sourceTopic)
                                .kv("destType", destination.getRight()).kv("destTopic", destination.getLeft())
                                .log("Failed to publish");
                    }
                }
            }
        }
    }

    private void processMappingAndSubscribe() {
        LOGGER.atDebug().kv("topicMapping", topicMapping).log("Processing mapping");

        List<TopicMapping.MappingEntry> mappingEntryList = topicMapping.getMapping();

        Map<TopicMapping.TopicType, Map<String, List<Pair<String, TopicMapping.TopicType>>>>
                perClientSourceDestinationMapTemp = new HashMap<>();

        mappingEntryList.forEach(mappingEntry -> {
            // Ensure mapping for client type
            Map<String, List<Pair<String, TopicMapping.TopicType>>> sourceDestinationMap =
                    perClientSourceDestinationMapTemp
                            .computeIfAbsent(mappingEntry.getSourceTopicType(), k -> new HashMap<>());

            // Add destinations for each source topic
            sourceDestinationMap.computeIfAbsent(mappingEntry.getSourceTopic(), k -> new ArrayList<>())
                    .add(new Pair<>(mappingEntry.getDestTopic(), mappingEntry.getDestTopicType()));
        });

        perClientSourceDestinationMap = perClientSourceDestinationMapTemp;

        messageClientMap.forEach(this::updateSubscriptionsForClient);
        LOGGER.atDebug().kv("topicMapping", perClientSourceDestinationMap).log("Processed mapping");
    }

    private synchronized void updateSubscriptionsForClient(TopicMapping.TopicType clientType,
                                                           MessageClient messageClient) {
        Map<String, List<Pair<String, TopicMapping.TopicType>>> srcDestMapping =
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
