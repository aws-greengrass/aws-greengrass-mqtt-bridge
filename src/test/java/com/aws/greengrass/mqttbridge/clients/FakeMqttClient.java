/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge.clients;

import lombok.Getter;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.eclipse.paho.client.mqttv3.MqttTopic;

import java.util.ArrayList;
import java.util.List;

public class FakeMqttClient implements IMqttClient {
    // To appease PMD
    private static final String UNSUPPORTED_OPERATION = "Unsupported operation";

    MqttCallback mqttCallback;
    String clientId;

    @Getter
    List<String> subscriptionTopics; // TODO: Support QoS

    @Getter
    List<TopicMessagePair> publishedMessages;

    @Getter
    MqttConnectOptions connectOptions;
    @Getter
    int connectCount = 0;
    final Object connectMonitor;
    boolean isConnected;

    public class TopicMessagePair {
        @Getter
        String topic;
        @Getter
        MqttMessage message;

        TopicMessagePair(String topic, MqttMessage message) {
            this.topic = topic;
            this.message = message;
        }
    }

    FakeMqttClient(String clientId) {
        this.clientId = clientId;
        this.subscriptionTopics = new ArrayList<>();
        this.publishedMessages = new ArrayList<>();
        this.connectMonitor = new Object();
    }

    /**
     * Simulate a message received from the broker. This method will invoke the messageArrived callback.
     *
     * @param topic   MQTT topic the message is received on
     * @param message MQTT message
     * @throws Exception if the client message received callback throws an exception
     */
    @SuppressWarnings("PMD.SignatureDeclareThrowsException")
    void injectMessage(String topic, MqttMessage message) throws Exception {
        mqttCallback.messageArrived(topic, message);
    }

    /**
     * Simulate a connection loss.
     *
     * @throws MqttException if the client connection lost callback throws an exception
     */
    void injectConnectionLoss() throws MqttException {
        disconnect();
        mqttCallback.connectionLost(new MqttException(MqttException.REASON_CODE_CONNECTION_LOST));
    }

    /**
     * Wait for the mqtt client to connect to the broker.
     *
     * @param timeout timeout in milliseconds
     * @return true if the client is connected (or becomes connected) before the given timeout, else false.
     */
    boolean waitForConnect(int timeout) {
        synchronized (connectMonitor) {
            if (isConnected()) {
                return true;
            }

            try {
                connectMonitor.wait(timeout);
            } catch (InterruptedException e) {
                return isConnected();
            }

            return true;
        }
    }

    @Override
    public void connect() throws MqttSecurityException, MqttException {
        isConnected = true;
        connectCount++;
        synchronized (connectMonitor) {
            connectMonitor.notifyAll();
        }
    }

    @Override
    public void connect(MqttConnectOptions mqttConnectOptions) throws MqttSecurityException, MqttException {
        this.connectOptions = mqttConnectOptions;
        connect();
    }

    @Override
    public IMqttToken connectWithResult(MqttConnectOptions mqttConnectOptions)
            throws MqttSecurityException, MqttException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    @Override
    public void disconnect() throws MqttException {
        isConnected = false;
        // Reset subscriptions
        subscriptionTopics.clear();
    }

    @Override
    public void disconnect(long disconnectTimeout) throws MqttException {
        disconnect();
    }

    @Override
    public void disconnectForcibly() throws MqttException {
        disconnect();
    }

    @Override
    public void disconnectForcibly(long disconnectTimeout) throws MqttException {
        disconnectForcibly();
    }

    @Override
    public void disconnectForcibly(long quiesceTimeout, long disconnectTimeout) throws MqttException {
        disconnectForcibly();
    }

    @Override
    public void subscribe(String topicFilter) throws MqttException, MqttSecurityException {
        subscribe(topicFilter, 1);
    }

    @Override
    public void subscribe(String[] topicFilters) throws MqttException {
        for (String topicFilter : topicFilters) {
            subscribe(topicFilter);
        }
    }

    @Override
    public void subscribe(String topicFilter, int qos) throws MqttException {
        if (!subscriptionTopics.contains(topicFilter)) {
            subscriptionTopics.add(topicFilter);
        }
    }

    @Override
    public void subscribe(String[] topicFilters, int[] qos) throws MqttException {
        if (topicFilters.length != qos.length) {
            throw new IllegalArgumentException("Topic filter and qos array lengths must match");
        }
        for (int i=0; i< topicFilters.length; i++) {
            subscribe(topicFilters[i], qos[i]);
        }
    }

    @Override
    public void subscribe(String topicFilter, IMqttMessageListener iMqttMessageListener)
            throws MqttException, MqttSecurityException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    @Override
    public void subscribe(String[] topicFilters, IMqttMessageListener[] iMqttMessageListeners) throws MqttException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    @Override
    public void subscribe(String topicFilter, int qos, IMqttMessageListener iMqttMessageListener) throws MqttException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    @Override
    public void subscribe(String[] topicFilters, int[] qos, IMqttMessageListener[] iMqttMessageListeners)
            throws MqttException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    @Override
    public IMqttToken subscribeWithResponse(String topicFilter) throws MqttException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    @Override
    public IMqttToken subscribeWithResponse(String topicFilter, IMqttMessageListener iMqttMessageListener) throws MqttException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    @Override
    public IMqttToken subscribeWithResponse(String topicFilter, int qos) throws MqttException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    @Override
    public IMqttToken subscribeWithResponse(String topicFilter, int qos, IMqttMessageListener iMqttMessageListener)
            throws MqttException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    @Override
    public IMqttToken subscribeWithResponse(String[] topicFilters) throws MqttException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    @Override
    public IMqttToken subscribeWithResponse(String[] topicFilters, IMqttMessageListener[] iMqttMessageListeners)
            throws MqttException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    @Override
    public IMqttToken subscribeWithResponse(String[] topicFilters, int[] qos) throws MqttException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    @Override
    public IMqttToken subscribeWithResponse(String[] topicFilters, int[] qos, IMqttMessageListener[] iMqttMessageListeners)
            throws MqttException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    @Override
    public void unsubscribe(String topicFilter) throws MqttException {
        subscriptionTopics.remove(topicFilter);
    }

    @Override
    public void unsubscribe(String[] topicFilters) throws MqttException {
        for (String topicFilter : topicFilters) {
            unsubscribe(topicFilter);
        }
    }

    @Override
    public void publish(String topicFilter, byte[] bytes, int qos, boolean retained) throws MqttException,
            MqttPersistenceException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    @Override
    public void publish(String topic, MqttMessage mqttMessage) throws MqttException, MqttPersistenceException {
        publishedMessages.add(new TopicMessagePair(topic, mqttMessage));
    }

    @Override
    public void setCallback(MqttCallback mqttCallback) {
        this.mqttCallback = mqttCallback;
    }

    @Override
    public MqttTopic getTopic(String topic) {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    @Override
    public boolean isConnected() {
        return isConnected;
    }

    @Override
    public String getClientId() {
        return clientId;
    }

    @Override
    public String getServerURI() {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    @Override
    public IMqttDeliveryToken[] getPendingDeliveryTokens() {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    @Override
    public void setManualAcks(boolean manualAcks) {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    @Override
    public void reconnect() throws MqttException {
        disconnect();
        connect();
    }

    @Override
    public void messageArrivedComplete(int messageId, int qos) throws MqttException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    @Override
    public void close() throws MqttException {
        disconnect();
    }
}
