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

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import javax.inject.Inject;

public class MQTTClient implements MessageClient {
    private static final Logger LOGGER = LogManager.getLogger(MQTTClient.class);
    private static final String DEFAULT_BROKER_URI = "tcp://localhost:8883";
    public static final String BROKER_URI_KEY = "brokerServerUri";
    public static final String CLIENT_ID_KEY = "clientId";
    public static final String TOPIC = "topic";

    private Consumer<Message> messageHandler;
    private final String serverUri;
    private final String clientId;

    private final MqttClientPersistence dataStore;
    private MqttClient mqttClientInternal;
    @Getter(AccessLevel.PROTECTED)
    private Set<String> subscribedLocalMqttTopics = new HashSet<>();

    private final MqttCallback mqttCallback = new MqttCallback() {
        @Override
        public void connectionLost(Throwable cause) {
            LOGGER.atDebug().setCause(cause).log("Mqtt client disconnected, reconnecting...");
            // TODO: Need to handle reconnects here, for now we try to reconnect once
            // TODO: If connection attempts fail, we should set the service to errored state
            reconnectAndResubscribe();
        }

        @Override
        public void messageArrived(String topic, org.eclipse.paho.client.mqttv3.MqttMessage message) {
            LOGGER.atTrace().kv(TOPIC, topic).log("Received MQTT message");

            if (messageHandler == null) {
                LOGGER.atWarn().kv(TOPIC, topic).log("Mqtt message received but message handler not set");
            } else {
                Message msg = new Message(topic, message.getPayload());
                messageHandler.accept(msg);
            }
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
        }
    };

    /**
     * Ctr for MQTTClient.
     *
     * @param topics topics passed in by kernel
     * @throws MQTTClientException if unable to create client for the mqtt broker
     */
    @Inject
    public MQTTClient(Topics topics) throws MQTTClientException {
        this(topics, null);
        // TODO: Handle the case when serverUri is modified
        try {
            this.mqttClientInternal = new MqttClient(serverUri, clientId, dataStore);
        } catch (MqttException e) {
            throw new MQTTClientException("Unable to create a MQTT client", e);
        }
    }

    protected MQTTClient(Topics topics, MqttClient mqttClient) {
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
            // TODO: need retry logic here if we want to remove dependency on broker
            mqttClientInternal.connect(connOpts);
        } catch (MqttException e) {
            LOGGER.atError().kv("uri", serverUri).kv(CLIENT_ID_KEY, clientId).log("Unable to connect to broker");
            throw new MQTTClientException("Unable to connect to MQTT broker", e);
        }
        LOGGER.atInfo().kv("uri", serverUri).kv(CLIENT_ID_KEY, clientId).log("Connected to broker");

        mqttClientInternal.setCallback(mqttCallback);
    }

    /**
     * Stop the {@link MQTTClient}.
     */
    public void stop() {
        removeMappingAndSubscriptions();

        try {
            mqttClientInternal.disconnect();
            dataStore.close();
        } catch (MqttException e) {
            LOGGER.atError().setCause(e).log("Failed to disconnect MQTT Client");
        }
    }

    private synchronized void removeMappingAndSubscriptions() {
        unsubscribeAll();
        subscribedLocalMqttTopics.clear();
    }

    private void unsubscribeAll() {
        LOGGER.atDebug().kv("mapping", subscribedLocalMqttTopics).log("unsubscribe from local mqtt topics");

        this.subscribedLocalMqttTopics.forEach(s -> {
            try {
                mqttClientInternal.unsubscribe(s);
                LOGGER.atDebug().kv(TOPIC, s).log("Unsubscribed to topic");
            } catch (MqttException e) {
                LOGGER.atWarn().kv(TOPIC, s).setCause(e).log("Unable to unsubscribe");
            }
        });
    }

    @Override
    public void publish(Message message) throws MessageClientException {
        try {
            mqttClientInternal
                    .publish(message.getTopic(), new org.eclipse.paho.client.mqttv3.MqttMessage(message.getPayload()));
        } catch (MqttException e) {
            LOGGER.atError().setCause(e).kv(TOPIC, message.getTopic()).log("MQTT Publish failed");
            throw new MQTTClientException("Failed to publish message", e);
        }
    }

    @Override
    public synchronized void updateSubscriptions(Set<String> topics, Consumer<Message> messageHandler) {
        updateSubscriptionsInternal(topics, messageHandler);
    }

    private void updateSubscriptionsInternal(Set<String> topics, Consumer<Message> messageHandler) {
        LOGGER.atDebug().kv("topics", topics).log("Subscribing to local mqtt topics");

        this.messageHandler = messageHandler;

        Set<String> topicsToRemove = new HashSet<>(subscribedLocalMqttTopics);
        topicsToRemove.removeAll(topics);
        topicsToRemove.forEach(s -> {
            try {
                mqttClientInternal.unsubscribe(s);
                LOGGER.atDebug().kv(TOPIC, s).log("Unsubscribed to topic");
                subscribedLocalMqttTopics.remove(s);
            } catch (MqttException e) {
                LOGGER.atError().kv(TOPIC, s).setCause(e).log("Unable to unsubscribe");
                // If we are unable to unsubscribe, leave the topic in the set so that we can try to remove next time.
            }
        });

        Set<String> topicsToSubscribe = new HashSet<>(topics);
        topicsToSubscribe.removeAll(subscribedLocalMqttTopics);

        // TODO: Support configurable qos, add retry
        topicsToSubscribe.forEach(s -> {
            try {
                mqttClientInternal.subscribe(s);
                LOGGER.atDebug().kv(TOPIC, s).log("Subscribed to topic");
                subscribedLocalMqttTopics.add(s);
            } catch (MqttException e) {
                LOGGER.atError().kv(TOPIC, s).log("Failed to subscribe");
            }
        });
    }

    private void reconnectAndResubscribe() {
        try {
            mqttClientInternal.reconnect();
        } catch (MqttException e) {
            LOGGER.atError().setCause(e).log("Unable to create a MQTT client");
            return;
        }

        resubscribe();
    }

    private synchronized void resubscribe() {
        Set<String> topicsToResubscribe = new HashSet<>(subscribedLocalMqttTopics);
        subscribedLocalMqttTopics.clear();
        // Resubscribe to topics
        updateSubscriptionsInternal(topicsToResubscribe, messageHandler);
    }
}

