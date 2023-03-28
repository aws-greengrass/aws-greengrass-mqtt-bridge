/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.clients;


import com.aws.greengrass.mqtt.bridge.BridgeConfig;
import com.aws.greengrass.mqtt.bridge.auth.MQTTClientKeyStore;
import com.aws.greengrass.mqtt.bridge.model.BridgeConfigReference;
import com.aws.greengrass.mqtt.bridge.model.MqttMessage;

import java.util.concurrent.ExecutorService;
import javax.inject.Inject;

public class LocalMqttClientFactory {

    private final BridgeConfigReference config;
    private final MQTTClientKeyStore mqttClientKeyStore;
    private final ExecutorService executorService;

    /**
     * Create a new LocalMqttClientFactory.
     *
     * @param config             bridge config
     * @param mqttClientKeyStore mqtt client key store
     * @param executorService    executor service
     */
    @Inject
    public LocalMqttClientFactory(BridgeConfigReference config,
                                  MQTTClientKeyStore mqttClientKeyStore,
                                  ExecutorService executorService) {
        this.config = config;
        this.mqttClientKeyStore = mqttClientKeyStore;
        this.executorService = executorService;
    }

    /**
     * Create a local mqtt client.
     *
     * @return local mqtt client
     * @throws MessageClientException if unable to create client
     */
    public MessageClient<MqttMessage> createLocalMqttClient() throws MessageClientException {
        BridgeConfig config = this.config.get();
        if (config == null) {
            throw new MessageClientException("Unable to create message client, bridge configuration not set");
        }
        switch (config.getMqttVersion()) {
            case MQTT5:
                return new LocalMqtt5Client(
                        config.getBrokerUri(),
                        config.getClientId(),
                        mqttClientKeyStore,
                        executorService
                );
            case MQTT3: // fall-through
            default:
                return new MQTTClient(config.getBrokerUri(), config.getClientId(),
                        mqttClientKeyStore, executorService);
        }
    }
}
