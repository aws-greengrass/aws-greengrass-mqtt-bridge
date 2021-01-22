/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge.clients;

import com.aws.greengrass.componentmanager.KernelConfigResolver;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqttbridge.MQTTBridge;
import com.aws.greengrass.mqttbridge.Message;
import com.aws.greengrass.mqttbridge.auth.MQTTClientKeyStore;
import com.aws.greengrass.util.Coerce;
import lombok.AccessLevel;
import lombok.Getter;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.security.KeyStoreException;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import javax.inject.Inject;
import javax.net.ssl.SSLSocketFactory;

public class MQTTClient implements MessageClient {
    private static final Logger LOGGER = LogManager.getLogger(MQTTClient.class);
    private static final String DEFAULT_BROKER_URI = "ssl://localhost:8883";
    public static final String BROKER_URI_KEY = "brokerServerUri";
    public static final String CLIENT_ID_KEY = "clientId";
    public static final String TOPIC = "topic";
    private static final int MIN_WAIT_RETRY_IN_SECONDS = 1;
    private static final int MAX_WAIT_RETRY_IN_SECONDS = 120;
    private static final int DEFAULT_MAX_RETRIES = 10;
    public static final String MAX_RETRIES_KEY = "brokerConnectMaxRetries";

    @Getter(AccessLevel.PACKAGE)
    private final MqttConnectOptions connOpts = new MqttConnectOptions();
    private Consumer<Message> messageHandler;
    private final String serverUri;
    private final String clientId;
    private final int maxRetries;

    private final MqttClientPersistence dataStore;
    private MqttClient mqttClientInternal;
    @Getter(AccessLevel.PROTECTED)
    private Set<String> subscribedLocalMqttTopics = new HashSet<>();

    private final MQTTClientKeyStore mqttClientKeyStore;

    private final MQTTBridge mqttBridge;

    private final MqttCallback mqttCallback = new MqttCallback() {
        @Override
        public void connectionLost(Throwable cause) {
            LOGGER.atDebug().setCause(cause).log("Mqtt client disconnected, reconnecting...");
            try {
                connectWithRetry();
                resubscribe();
            } catch (MQTTClientException e) {
                LOGGER.atError().setCause(e).log("Unable to reconnect to broker");
                mqttBridge.serviceErrored(e);
            }
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
     * @param mqttBridge         MQTTBridge service class instance
     * @param topics             topics passed in by Nucleus
     * @param mqttClientKeyStore KeyStore for MQTT Client
     * @throws MQTTClientException if unable to create client for the mqtt broker
     */
    @Inject
    public MQTTClient(MQTTBridge mqttBridge, Topics topics, MQTTClientKeyStore mqttClientKeyStore)
            throws MQTTClientException {
        this(mqttBridge, topics, mqttClientKeyStore, null);
        // TODO: Handle the case when serverUri is modified
        try {
            this.mqttClientInternal = new MqttClient(serverUri, clientId, dataStore);
        } catch (MqttException e) {
            throw new MQTTClientException("Unable to create a MQTT client", e);
        }
    }

    protected MQTTClient(MQTTBridge mqttBridge, Topics topics, MQTTClientKeyStore mqttClientKeyStore,
                         MqttClient mqttClient) {
        this.mqttBridge = mqttBridge;
        this.mqttClientInternal = mqttClient;
        this.dataStore = new MemoryPersistence();
        this.serverUri = Coerce.toString(topics.findOrDefault(DEFAULT_BROKER_URI,
                KernelConfigResolver.CONFIGURATION_CONFIG_KEY, BROKER_URI_KEY));
        this.clientId = Coerce.toString(topics.findOrDefault(MQTTBridge.SERVICE_NAME,
                KernelConfigResolver.CONFIGURATION_CONFIG_KEY, CLIENT_ID_KEY));
        this.maxRetries = Coerce.toInt(topics.findOrDefault(DEFAULT_MAX_RETRIES,
                KernelConfigResolver.CONFIGURATION_CONFIG_KEY, MAX_RETRIES_KEY));
        this.mqttClientKeyStore = mqttClientKeyStore;
        this.mqttClientKeyStore.listenToUpdates(this::readKeyStore);
    }

    private void readKeyStore() {
        try {
            if (serverUri.startsWith("ssl")) {
                SSLSocketFactory ssf = mqttClientKeyStore.getSSLSocketFactory();
                connOpts.setSocketFactory(ssf);
            }
        } catch (KeyStoreException e) {
            LOGGER.atError().setCause(e).log("Unable to read updated keystore");
        }
    }

    /**
     * Start the {@link MQTTClient}.
     *
     * @throws MQTTClientException if unable to connect to the broker
     * @throws KeyStoreException   if unable to read keystore
     */
    public void start() throws MQTTClientException, KeyStoreException {
        connOpts.setCleanSession(true);
        if (serverUri.startsWith("ssl")) {
            SSLSocketFactory ssf = mqttClientKeyStore.getSSLSocketFactory();
            connOpts.setSocketFactory(ssf);
        }

        LOGGER.atInfo().kv("uri", serverUri).kv(CLIENT_ID_KEY, clientId).log("Connecting to broker");
        connectWithRetry();
        LOGGER.atInfo().kv("uri", serverUri).kv(CLIENT_ID_KEY, clientId).log("Connected to broker");

        mqttClientInternal.setCallback(mqttCallback);
    }

    /**
     * Stop the {@link MQTTClient}.
     */
    public void stop() {
        removeMappingAndSubscriptions();

        try {
            if (mqttClientInternal.isConnected()) {
                mqttClientInternal.disconnect();
            }
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

    private synchronized void resubscribe() {
        Set<String> topicsToResubscribe = new HashSet<>(subscribedLocalMqttTopics);
        subscribedLocalMqttTopics.clear();
        // Resubscribe to topics
        updateSubscriptionsInternal(topicsToResubscribe, messageHandler);
    }

    @SuppressWarnings("PMD.PreserveStackTrace")
    private void connectWithRetry() throws MQTTClientException {
        int waitBeforeRetry = MIN_WAIT_RETRY_IN_SECONDS;
        int numRetries = 0;

        while (!mqttClientInternal.isConnected() && numRetries < maxRetries) {
            try {
                mqttClientInternal.connect(connOpts);
            } catch (MqttException e) {
                LOGGER.atDebug().setCause(e)
                        .log("Unable to connect. Will be retried after {} seconds", waitBeforeRetry);
                try {
                    Thread.sleep(waitBeforeRetry * 1000);
                } catch (InterruptedException ex) {
                    throw new MQTTClientException("Interrupted while waiting to retry connect", ex);
                }
                waitBeforeRetry = Math.min(2 * waitBeforeRetry, MAX_WAIT_RETRY_IN_SECONDS);
            }
            numRetries++;
        }

        if (!mqttClientInternal.isConnected()) {
            throw new MQTTClientException(String.format("Unable to connect to broker after %d retries", maxRetries));
        }
    }
}
