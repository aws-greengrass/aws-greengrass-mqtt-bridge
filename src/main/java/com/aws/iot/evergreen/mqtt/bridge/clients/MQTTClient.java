/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.iot.evergreen.mqtt.bridge.clients;

import com.aws.iot.evergreen.config.Topics;
import com.aws.iot.evergreen.logging.api.Logger;
import com.aws.iot.evergreen.logging.impl.LogManager;
import com.aws.iot.evergreen.mqtt.bridge.MQTTBridge;
import com.aws.iot.evergreen.mqtt.bridge.Message;
import com.aws.iot.evergreen.mqtt.bridge.MessageBridge;
import com.aws.iot.evergreen.mqtt.bridge.TopicMapping;
import com.aws.iot.evergreen.util.Coerce;
import lombok.AccessLevel;
import lombok.Getter;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;

public class MQTTClient {
    private static final Logger LOGGER = LogManager.getLogger(MQTTClient.class);
    private static final String DEFAULT_BROKER_URI = "tcp://localhost:8883";
    public static final String BROKER_URI_KEY = "brokerServerUri";
    public static final String CLIENT_ID_KEY = "clientId";
    public static final String TOPIC = "topic";

    private final TopicMapping topicMapping;
    private final MessageBridge messageBridge;
    private final String serverUri;
    private final String clientId;

    private final MqttClientPersistence dataStore;
    private MqttClient mqttClientInternal;
    //  TODO: Optimize these mappings
    @Getter(AccessLevel.PACKAGE)
    private List<TopicMapping.MappingEntry> topicMappingsWithDestinationAsLocalMqtt = new ArrayList<>();
    @Getter(AccessLevel.PACKAGE)
    private List<TopicMapping.MappingEntry> subscribedLocalMqttTopics = new ArrayList<>();

    private final MessageBridge.MessageListener messageListener = (sourceType, msg) -> {
        List<TopicMapping.MappingEntry> topicMappingsToPublishTo = topicMappingsWithDestinationAsLocalMqtt.stream()
                .filter(mappingEntry -> mappingEntry.getSourceTopicType().equals(sourceType) && mappingEntry
                        .getSourceTopic().equals(msg.getTopic())).collect(Collectors.toList());
        for (TopicMapping.MappingEntry entry : topicMappingsToPublishTo) {
            // TODO: Support configurable qos
            try {
                mqttClientInternal.publish(entry.getDestTopic(),
                        new org.eclipse.paho.client.mqttv3.MqttMessage(msg.getPayload()));
            } catch (MqttException e) {
                LOGGER.atError().kv(TOPIC, entry.getDestTopic()).log("MQTT Publish failed");
            }
        }
    };

    private final MqttCallback mqttCallback = new MqttCallback() {
        @Override
        public void connectionLost(Throwable cause) {
            LOGGER.atTrace().setCause(cause).log("Mqtt client disconnected");
        }

        @Override
        public void messageArrived(String topic, org.eclipse.paho.client.mqttv3.MqttMessage message) {
            LOGGER.atTrace().kv(TOPIC, topic).log("Received MQTT message");

            if (subscribedLocalMqttTopics.stream()
                    .anyMatch(mappingEntry -> mappingEntry.getSourceTopic().equals(topic))) {
                Message msg = new Message(topic, message.getPayload());
                messageBridge.notifyMessage(msg, TopicMapping.TopicType.LocalMqtt);
            } else {
                LOGGER.atDebug().kv(TOPIC, topic).log("Mqtt message received at an unexpected topic");
            }
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
        }
    };

    /**
     * Ctr for MQTTClient.
     *
     * @param topics        topics passed in by kernel
     * @param topicMapping  Topic mapping
     * @param messageBridge Message bridge to route the messages
     * @throws MQTTClientException if unable to create client for the mqtt broker
     */
    @Inject
    public MQTTClient(Topics topics, TopicMapping topicMapping, MessageBridge messageBridge)
            throws MQTTClientException {
        this(topics, topicMapping, messageBridge, null);
        // TODO: Handle the case when serverUri is modified
        try {
            this.mqttClientInternal = new MqttClient(serverUri, clientId, dataStore);
        } catch (MqttException e) {
            throw new MQTTClientException("Unable to create a MQTT client", e);
        }
    }

    protected MQTTClient(Topics topics, TopicMapping topicMapping, MessageBridge messageBridge, MqttClient mqttClient) {
        this.topicMapping = topicMapping;
        this.messageBridge = messageBridge;
        this.mqttClientInternal = mqttClient;
        this.dataStore = new MemoryPersistence();
        this.serverUri = Coerce.toString(topics.findOrDefault(DEFAULT_BROKER_URI, BROKER_URI_KEY));
        this.clientId = Coerce.toString(topics.findOrDefault(MQTTBridge.SERVICE_NAME, CLIENT_ID_KEY));
    }

    /**
     * Start the {@link MQTTClient}.
     *
     * @throws MQTTClientException if unable to connect to the broker
     */
    public void start() throws MQTTClientException {
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        LOGGER.atInfo().kv("uri", serverUri).kv(CLIENT_ID_KEY, clientId).log("Connecting to broker");
        try {
            mqttClientInternal.connect(connOpts);
        } catch (MqttException e) {
            LOGGER.atError().kv("uri", serverUri).kv(CLIENT_ID_KEY, clientId).log("Unable to connect to broker");
            throw new MQTTClientException("Unable to connect to MQTT broker", e);
        }
        LOGGER.atInfo().kv("uri", serverUri).kv(CLIENT_ID_KEY, clientId).log("Connected to broker");

        mqttClientInternal.setCallback(mqttCallback);

        topicMapping.listenToUpdates(() -> {
            try {
                updateMappingAndSubscriptions();
            } catch (MQTTClientException e) {
                LOGGER.atError().setCause(e).log("Error while updating subscriptions");
            }
        });
        updateMappingAndSubscriptions();

        // We are interested in messages from IoTCore and Pubsub
        messageBridge.addListener(messageListener, TopicMapping.TopicType.IotCore);
        messageBridge.addListener(messageListener, TopicMapping.TopicType.Pubsub);
    }

    /**
     * Stop the {@link MQTTClient}.
     */
    public void stop() {
        messageBridge.removeListener(messageListener, TopicMapping.TopicType.IotCore);
        messageBridge.removeListener(messageListener, TopicMapping.TopicType.Pubsub);

        removeMappingAndSubscriptions();

        try {
            mqttClientInternal.disconnect();
            if (dataStore != null) {
                dataStore.close();
            }
        } catch (MqttException e) {
            LOGGER.atError().cause(e).log("Failed to disconnect MQTT Client");
        }
    }

    private synchronized void updateMappingAndSubscriptions() throws MQTTClientException {
        List<TopicMapping.MappingEntry> topicMappings = topicMapping.getMapping();
        List<TopicMapping.MappingEntry> topicMappingsWithSourceAsLocalMqtt = topicMappings.stream()
                .filter(mappingEntry -> mappingEntry.getSourceTopicType().equals(TopicMapping.TopicType.LocalMqtt))
                .collect(Collectors.toList());
        updateSubscriptions(topicMappingsWithSourceAsLocalMqtt);

        topicMappingsWithDestinationAsLocalMqtt = topicMappings.stream()
                .filter(mappingEntry -> mappingEntry.getDestTopicType().equals(TopicMapping.TopicType.LocalMqtt))
                .collect(Collectors.toList());
    }

    private void updateSubscriptions(List<TopicMapping.MappingEntry> newTopicMappingsWithSourceAsLocalMqtt)
            throws MQTTClientException {
        LOGGER.atDebug().kv("mapping", newTopicMappingsWithSourceAsLocalMqtt).log("Subscribing to local mqtt topics");

        List<TopicMapping.MappingEntry> entriesToRemove = new ArrayList<>();
        this.subscribedLocalMqttTopics.stream()
                .filter(mappingEntry -> !newTopicMappingsWithSourceAsLocalMqtt.contains(mappingEntry))
                .forEach(mappingEntry -> {
                    try {
                        mqttClientInternal.unsubscribe(mappingEntry.getSourceTopic());
                        LOGGER.atDebug().kv(TOPIC, mappingEntry.getSourceTopic()).log("Unsubscribed to topic");
                        entriesToRemove.add(mappingEntry);
                    } catch (MqttException e) {
                        LOGGER.atError().kv(TOPIC, mappingEntry.getSourceTopic()).setCause(e)
                                .log("Unable to unsubscribe");
                    }
                });

        subscribedLocalMqttTopics.removeAll(entriesToRemove);

        // TODO: Support configurable qos
        for (TopicMapping.MappingEntry entry : newTopicMappingsWithSourceAsLocalMqtt) {
            if (subscribedLocalMqttTopics.contains(entry)) {
                LOGGER.atDebug().kv(TOPIC, entry.getSourceTopic()).log("Already subscribed to topic");
                continue;
            }

            try {
                mqttClientInternal.subscribe(entry.getSourceTopic());
                LOGGER.atDebug().kv(TOPIC, entry.getSourceTopic()).log("Subscribed to topic");
                subscribedLocalMqttTopics.add(entry);
            } catch (MqttException e) {
                LOGGER.atError().kv(TOPIC, entry.getSourceTopic()).log("Failed to subscribe");
                throw new MQTTClientException(String.format("Failed to subscribe to topic %s", entry.getSourceTopic()),
                        e);
            }
        }
    }

    private synchronized void removeMappingAndSubscriptions() {
        unsubscribeAll();
        topicMappingsWithDestinationAsLocalMqtt.clear();
        subscribedLocalMqttTopics.clear();
    }

    private void unsubscribeAll() {
        LOGGER.atDebug().kv("mapping", subscribedLocalMqttTopics).log("unsubscribe from local mqtt topics");

        this.subscribedLocalMqttTopics.forEach(mappingEntry -> {
            try {
                mqttClientInternal.unsubscribe(mappingEntry.getSourceTopic());
                LOGGER.atDebug().kv(TOPIC, mappingEntry.getSourceTopic()).log("Unsubscribed to topic");
            } catch (MqttException e) {
                LOGGER.atError().kv(TOPIC, mappingEntry.getSourceTopic()).setCause(e).log("Unable to unsubscribe");
            }
        });
    }
}

