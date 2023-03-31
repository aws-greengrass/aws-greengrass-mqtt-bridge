/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests.extensions;

import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.mqtt.bridge.BridgeConfig;
import com.aws.greengrass.mqtt.bridge.MQTTBridge;
import com.aws.greengrass.mqtt.bridge.clients.IoTCoreClient;
import com.aws.greengrass.mqtt.bridge.clients.LocalMqtt5Client;
import com.aws.greengrass.mqtt.bridge.clients.MQTTClient;
import com.aws.greengrass.mqtt.bridge.clients.MessageClient;
import com.aws.greengrass.mqtt.bridge.clients.MockMqttClient;
import com.aws.greengrass.mqtt.bridge.model.BridgeConfigReference;
import com.aws.greengrass.mqtt.bridge.model.MqttMessage;
import lombok.Data;

import java.nio.file.Path;

@Data
public class BridgeIntegrationTestContext {
    Broker broker;
    Integer brokerSSLPort;
    Integer brokerTCPPort;
    String brokerHost;
    Path rootDir;
    Kernel kernel;

    public MockMqttClient getMockMqttClient() {
        return getFromContext(MockMqttClient.class);
    }

    public IoTCoreClient getIotCoreClient() {
        return getFromContext(MQTTBridge.class).getIotCoreClient();
    }

    public MQTTClient getLocalV3Client() {
        MessageClient<MqttMessage> client = getFromContext(MQTTBridge.class).getLocalMqttClient();
        if (!(client instanceof MQTTClient)) {
            throw new RuntimeException("Excepted " + MQTTClient.class.getSimpleName()
                    + " but got " + client.getClass().getSimpleName());
        }
        return (MQTTClient) client;
    }

    public LocalMqtt5Client getLocalV5Client() {
        MessageClient<MqttMessage> client = getFromContext(MQTTBridge.class).getLocalMqttClient();
        if (!(client instanceof LocalMqtt5Client)) {
            throw new RuntimeException("Excepted " + MQTTClient.class.getSimpleName()
                    + " but got " + client.getClass().getSimpleName());
        }
        return (LocalMqtt5Client) client;
    }

    public BridgeConfig getConfig() {
        return getFromContext(BridgeConfigReference.class).get();
    }

    public <T> T getFromContext(Class<T> clazz) {
        if (kernel == null) {
            throw new RuntimeException("Kernel not available. Ensure the test method is annotated with @WithKernel");
        }
        return kernel.getContext().get(clazz);
    }
}
