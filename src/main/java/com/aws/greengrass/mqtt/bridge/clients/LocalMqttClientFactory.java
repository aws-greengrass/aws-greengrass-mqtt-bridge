/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.clients;


import com.aws.greengrass.mqtt.bridge.BridgeConfig;
import com.aws.greengrass.mqtt.bridge.auth.MQTTClientKeyStore;
import com.aws.greengrass.mqtt.bridge.model.MqttMessage;
import lombok.Setter;

import java.util.concurrent.ExecutorService;
import javax.inject.Inject;

public class LocalMqttClientFactory {

    private final MQTTClientKeyStore mqttClientKeyStore;
    private final ExecutorService executorService;

    @Setter
    private BridgeConfig config;

    @Inject
    public LocalMqttClientFactory(MQTTClientKeyStore mqttClientKeyStore,
                                  ExecutorService executorService) {
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
        checkConfig();
        switch (config.getMqttVersion()) {
            case MQTT5:
                // TODO
            case MQTT3: // fall-through
            default:
                return new MQTTClient(config.getBrokerUri(), config.getClientId(),
                        mqttClientKeyStore, executorService);
        }
    }

    private void checkConfig() {
        if (config == null) {
            throw new IllegalStateException("config is missing, ensure setConfig() is called");
        }
    }
}
