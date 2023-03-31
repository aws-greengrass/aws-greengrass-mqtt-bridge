/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.clients;

import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.mqttclient.MqttRequestException;
import com.aws.greengrass.mqttclient.spool.SpoolerStoreException;
import com.aws.greengrass.mqttclient.v5.Publish;
import com.aws.greengrass.mqttclient.v5.PublishResponse;
import com.aws.greengrass.mqttclient.v5.Subscribe;
import com.aws.greengrass.mqttclient.v5.SubscribeResponse;
import com.aws.greengrass.mqttclient.v5.Unsubscribe;
import lombok.Getter;
import org.mockito.invocation.InvocationOnMock;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

public class MockMqttClient {

    private final AtomicBoolean online = new AtomicBoolean(true);
    private final Object subscriptionsLock = new Object();
    @Getter
    private final List<Subscribe> subscriptions = new ArrayList<>();
    private final Object publishLock = new Object();
    @Getter
    private final List<Publish> toPublish = new ArrayList<>();
    @Getter
    private final List<Publish> published = new ArrayList<>();

    @Getter
    private final boolean cleanSession;

    @Getter
    private final MqttClient mqttClient;

    public MockMqttClient(boolean cleanSession){
        this.cleanSession = cleanSession;
        this.mqttClient = mock(MqttClient.class);

        lenient().when(mqttClient.getMqttOnline()).thenReturn(online);
        try {
            lenient().doAnswer(this::onSubscribe).when(mqttClient).subscribe(any(Subscribe.class));
            lenient().doAnswer(this::onPublish).when(mqttClient).publish(any(Publish.class));
            lenient().doAnswer(this::onUnsubscribe).when(mqttClient).unsubscribe(any(Unsubscribe.class));
        } catch (MqttRequestException | SpoolerStoreException | InterruptedException e) {
            fail(e);
        }
    }

    private CompletableFuture<SubscribeResponse> onSubscribe(InvocationOnMock invocation) {
        if (!online.get()) {
            CompletableFuture<SubscribeResponse> resp = new CompletableFuture<>();
            resp.completeExceptionally(new RuntimeException("subscription failed, not connected"));
            return resp;
        }
        Subscribe subscribe = invocation.getArgument(0);
        synchronized (subscriptionsLock) {
            subscriptions.add(subscribe);
        }
        return CompletableFuture.completedFuture(
                new SubscribeResponse("", 0, null));
    }

    private PublishResponse onPublish(InvocationOnMock invocation) {
        Publish publish = invocation.getArgument(0);
        toPublish.add(publish);
        flush();
        return new PublishResponse();
    }

    private CompletableFuture<Void> onUnsubscribe(InvocationOnMock invocation) {
        if (!online.get()) {
            CompletableFuture<Void> resp = new CompletableFuture<>();
            resp.completeExceptionally(new RuntimeException("unsubscribe failed, not connected"));
            return resp;
        }
        Unsubscribe unsubscribe = invocation.getArgument(0);
        synchronized (subscriptionsLock) {
            subscriptions.removeIf(s -> s.getTopic().equals(unsubscribe.getTopic()));
        }
        return CompletableFuture.completedFuture(null);
    }

    private void flush() {
        synchronized (publishLock) {
            Iterator<Publish> iter = toPublish.iterator();
            while (iter.hasNext()) {
                if (!online.get()) {
                    return;
                }
                Publish publish = iter.next();
                published.add(publish);
                synchronized (subscriptionsLock) {
                    for (Subscribe subscribe : subscriptions) {
                        if (!subscribe.getTopic().equals(publish.getTopic())) {
                            continue;
                        }
                        if (subscribe.isNoLocal()) {
                            continue;
                        }
                        subscribe.getCallback().accept(publish);
                    }
                }
                iter.remove();
            }
        }
    }

    public void online() {
        online.set(true);
        onConnect();
        flush();
    }

    public void offline() {
        online.set(false);
    }

    private void onConnect() {
        if (cleanSession) {
            synchronized (subscriptionsLock) {
                subscriptions.clear();
            }
        }
    }
}
