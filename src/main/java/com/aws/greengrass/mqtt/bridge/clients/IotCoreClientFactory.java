/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.clients;


import com.aws.greengrass.mqtt.bridge.BridgeConfig;
import com.aws.greengrass.mqtt.bridge.TopicMapping;
import com.aws.greengrass.mqtt.bridge.model.BridgeConfigReference;
import com.aws.greengrass.mqttclient.MqttClient;

import java.util.concurrent.ExecutorService;
import javax.inject.Inject;

public class IotCoreClientFactory {

    private final BridgeConfigReference config;
    private final MqttClient mqttClient;
    private final ExecutorService executorService;

    /**
     * Create a new IotCoreClientFactory.
     *
     * @param mqttClient      mqtt client
     * @param executorService executor service
     * @param config          bridge config
     */
    @Inject
    public IotCoreClientFactory(MqttClient mqttClient,
                                ExecutorService executorService,
                                BridgeConfigReference config) {
        this.mqttClient = mqttClient;
        this.executorService = executorService;
        this.config = config;
    }

    /**
     * Create an iot core client.
     *
     * @return iot core client
     * @throws MessageClientException if unable to create client
     */
    public IoTCoreClient createIotCoreClient() throws MessageClientException {
        BridgeConfig config = this.config.get();
        if (config == null) {
            throw new MessageClientException("Unable to create message client, bridge configuration not set");
        }
        return new IoTCoreClient(
                mqttClient,
                executorService,
                config.getMqtt5RouteOptionsForSource(TopicMapping.TopicType.IotCore)
        );
    }
}
