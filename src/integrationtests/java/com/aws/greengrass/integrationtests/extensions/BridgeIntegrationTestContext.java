/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests.extensions;

import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.mqtt.bridge.BridgeConfig;
import com.aws.greengrass.mqtt.bridge.MQTTBridge;
import com.aws.greengrass.mqtt.bridge.TopicMapping;
import com.aws.greengrass.mqtt.bridge.clients.IoTCoreClient;
import com.aws.greengrass.mqtt.bridge.clients.LocalMqtt5Client;
import com.aws.greengrass.mqtt.bridge.clients.MQTTClient;
import com.aws.greengrass.mqtt.bridge.clients.MessageClient;
import com.aws.greengrass.mqtt.bridge.clients.MessageClientException;
import com.aws.greengrass.mqtt.bridge.clients.MessageClients;
import com.aws.greengrass.mqtt.bridge.clients.MockMqttClient;
import com.aws.greengrass.mqtt.bridge.model.BridgeConfigReference;
import com.aws.greengrass.mqtt.bridge.model.Message;
import com.aws.greengrass.mqtt.bridge.model.MqttMessage;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.fail;

@Data
public class BridgeIntegrationTestContext {
    Broker broker;
    Integer brokerSSLPort;
    Integer brokerTCPPort;
    String brokerHost;
    @Getter(AccessLevel.NONE)
    Runnable stopBroker;
    @Getter(AccessLevel.NONE)
    Runnable startBroker;
    Path rootDir;
    Kernel kernel;
    Certs certs;

    public MockMqttClient getMockMqttClient() {
        return getFromContext(MockMqttClient.class);
    }

    public IoTCoreClient getIotCoreClient() {
        return getFromContext(IoTCoreClient.class);
    }

    public MQTTClient getLocalV3Client() {
        MessageClient<? extends Message> client = getLocalClient();
        if (!(client instanceof MQTTClient)) {
            throw new RuntimeException("Excepted " + MQTTClient.class.getSimpleName()
                    + " but got " + client.getClass().getSimpleName());
        }
        return (MQTTClient) client;
    }

    public LocalMqtt5Client getLocalV5Client() {
        MessageClient<? extends Message> client = getLocalClient();
        if (!(client instanceof LocalMqtt5Client)) {
            throw new RuntimeException("Excepted " + MQTTClient.class.getSimpleName()
                    + " but got " + client.getClass().getSimpleName());
        }
        return (LocalMqtt5Client) client;
    }

    private MessageClient<? extends Message> getLocalClient() {
        try {
            return getFromContext(MessageClients.class).getMessageClients().stream()
                    .filter(c -> c.getType().equals(TopicMapping.TopicType.LocalMqtt))
                    .findFirst()
                    .get();
        } catch (MessageClientException e) {
            fail(e);
            return null;
        }
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

    public void startBroker() {
        if (startBroker == null) {
            fail("startBroker operation has not been set");
        }
        startBroker.run();
    }

    public void stopBroker() {
        if (stopBroker == null) {
            fail("stopBroker operation has not been set");
        }
        stopBroker.run();
    }
}
