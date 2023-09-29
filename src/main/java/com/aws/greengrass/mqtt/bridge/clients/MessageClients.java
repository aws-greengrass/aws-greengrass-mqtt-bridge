/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.clients;

import com.aws.greengrass.mqtt.bridge.model.Message;
import com.aws.greengrass.mqtt.bridge.model.MqttMessage;
import com.aws.greengrass.mqtt.bridge.model.PubSubMessage;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.inject.Inject;

public class MessageClients implements Iterable<MessageClient<? extends Message>> {

    private final MessageClient<MqttMessage> iotCoreClient;
    private final MessageClientFactory<MqttMessage> localMqttClientFactory;
    private final MessageClient<PubSubMessage> pubSubClient;

    private List<MessageClient<? extends Message>> clients;


    /**
     * Construct MessageClients.
     *
     * @param iotCoreClient          iot core client
     * @param localMqttClientFactory local mqtt client factory
     * @param pubSubClient           pub sub client
     */
    @Inject
    public MessageClients(IoTCoreClient iotCoreClient,
                          LocalMqttClientFactory localMqttClientFactory,
                          PubSubClient pubSubClient) {
        this.iotCoreClient = iotCoreClient;
        this.localMqttClientFactory = localMqttClientFactory;
        this.pubSubClient = pubSubClient;
    }

    /**
     * Construct MessageClients.
     *
     * @param iotCoreClient   iot core client
     * @param localMqttClient local mqtt client
     * @param pubSubClient    pub sub client
     */
    // for testing
    public MessageClients(MessageClient<MqttMessage> iotCoreClient,
                          MessageClient<MqttMessage> localMqttClient,
                          MessageClient<PubSubMessage> pubSubClient) {
        this.iotCoreClient = iotCoreClient;
        this.localMqttClientFactory = () -> localMqttClient;
        this.pubSubClient = pubSubClient;
    }

    /**
     * Get all bridge message clients, creating them only if necessary.
     *
     * @return message clients
     * @throws MessageClientException if unable to create message clients
     */
    public List<MessageClient<? extends Message>> getMessageClients()
            throws MessageClientException {
        return getMessageClients(true);
    }

    /**
     * Get all bridge message clients, creating them if necessary or requested.
     *
     * @param cached whether to use cached message clients
     * @return message clients
     * @throws MessageClientException if unable to create message clients
     */
    public List<MessageClient<? extends Message>> getMessageClients(boolean cached) throws MessageClientException {
        if (!cached || clients == null) {
            clients = createMessageClients();
        }
        return clients;
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
