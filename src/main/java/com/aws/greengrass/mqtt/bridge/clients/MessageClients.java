/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.clients;

import com.aws.greengrass.mqtt.bridge.model.Message;
import com.aws.greengrass.mqtt.bridge.model.MqttMessage;
import com.aws.greengrass.mqtt.bridge.model.PubSubMessage;
import lombok.NonNull;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MessageClients implements Iterable<MessageClient<? extends Message>> {

    private final MessageClient<MqttMessage> iotCoreClient;
    private final MessageClientFactory<MqttMessage> localMqttClientFactory;
    private final MessageClient<PubSubMessage> pubSubClient;

    private List<MessageClient<? extends Message>> messageClients;



    @Inject
    public MessageClients(IoTCoreClient iotCoreClient,
                          LocalMqttClientFactory localMqttClientFactory,
                          PubSubClient pubSubClient) {
        this.iotCoreClient = iotCoreClient;
        this.localMqttClientFactory = localMqttClientFactory;
        this.pubSubClient = pubSubClient;
    }

    // for testing
    public MessageClients(MessageClient<MqttMessage> iotCoreClient,
                   MessageClient<MqttMessage> localMqttClient,
                   MessageClient<PubSubMessage> pubSubClient) {
        this.iotCoreClient = iotCoreClient;
        this.localMqttClientFactory = () -> localMqttClient;
        this.pubSubClient = pubSubClient;
    }

    public List<MessageClient<? extends Message>> getMessageClients()
            throws MessageClientException {
        return getMessageClients(true);
    }

    public List<MessageClient<? extends Message>> getMessageClients(boolean cached) throws MessageClientException {
        if (!cached || messageClients == null) {
            messageClients = createMessageClients();
        }
        return messageClients;
    }

    private List<MessageClient<? extends Message>> createMessageClients()
            throws MessageClientException {
        List<MessageClient<? extends Message>> clients = new ArrayList<>();
        clients.add(iotCoreClient);
        clients.add(localMqttClientFactory.createLocalMqttClient());
        clients.add(pubSubClient);
        return clients;
    }

    @NonNull
    @Override
    public Iterator<MessageClient<? extends Message>> iterator() {
        try {
            return getMessageClients().iterator();
        } catch (MessageClientException e) {
            throw new RuntimeException(e);
        }
    }
}
