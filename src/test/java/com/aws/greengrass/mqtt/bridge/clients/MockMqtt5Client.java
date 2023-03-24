/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.clients;

import lombok.Getter;
import lombok.NonNull;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.crt.mqtt5.Mqtt5Client;
import software.amazon.awssdk.crt.mqtt5.Mqtt5ClientOptions;
import software.amazon.awssdk.crt.mqtt5.OnAttemptingConnectReturn;
import software.amazon.awssdk.crt.mqtt5.OnConnectionSuccessReturn;
import software.amazon.awssdk.crt.mqtt5.OnDisconnectionReturn;
import software.amazon.awssdk.crt.mqtt5.PublishReturn;
import software.amazon.awssdk.crt.mqtt5.packets.ConnAckPacket;
import software.amazon.awssdk.crt.mqtt5.packets.DisconnectPacket;
import software.amazon.awssdk.crt.mqtt5.packets.PubAckPacket;
import software.amazon.awssdk.crt.mqtt5.packets.PublishPacket;
import software.amazon.awssdk.crt.mqtt5.packets.SubAckPacket;
import software.amazon.awssdk.crt.mqtt5.packets.SubscribePacket;
import software.amazon.awssdk.crt.mqtt5.packets.UnsubAckPacket;
import software.amazon.awssdk.crt.mqtt5.packets.UnsubscribePacket;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class MockMqtt5Client {

    private final AtomicBoolean online = new AtomicBoolean(true);
    private final Object subscriptionsLock = new Object();
    @Getter
    private final List<SubscribePacket> subscriptions = new ArrayList<>();
    private final Object publishLock = new Object();
    @Getter
    private final List<PublishPacket> toPublish = new ArrayList<>();
    @Getter
    private final List<PublishPacket> published = new ArrayList<>();
    @Getter
    private final boolean cleanSession;
    @Getter
    private final Mqtt5Client client = mock(Mqtt5Client.class);
    private final Mqtt5ClientOptions.LifecycleEvents lifecycleEvents;
    private final Mqtt5ClientOptions.PublishEvents publishEvents;

    public MockMqtt5Client(@NonNull Mqtt5ClientOptions.LifecycleEvents lifecycleEvents,
                           @NonNull Mqtt5ClientOptions.PublishEvents publishEvents) {
        this(lifecycleEvents, publishEvents, true);
    }

    public MockMqtt5Client(@NonNull Mqtt5ClientOptions.LifecycleEvents lifecycleEvents,
                           @NonNull Mqtt5ClientOptions.PublishEvents publishEvents,
                           boolean cleanSession) {
        this.lifecycleEvents = lifecycleEvents;
        this.publishEvents = publishEvents;
        this.cleanSession = cleanSession;

        lenient().doAnswer(ignored -> {
            online();
            return null;
        }).when(client).start();

        lenient().doAnswer(ignored -> {
            offline();
            return null;
        }).when(client).stop(any());

        lenient().when(client.getIsConnected()).thenAnswer((Answer<Boolean>) invocationOnMock -> online.get());
        lenient().doAnswer(this::onSubscribe).when(client).subscribe(any(SubscribePacket.class));
        lenient().doAnswer(this::onPublish).when(client).publish(any(PublishPacket.class));
        lenient().doAnswer(this::onUnsubscribe).when(client).unsubscribe(any(UnsubscribePacket.class));
    }

    private CompletableFuture<SubAckPacket> onSubscribe(InvocationOnMock invocation) {
        if (!online.get()) {
            CompletableFuture<SubAckPacket> resp = new CompletableFuture<>();
            resp.completeExceptionally(new RuntimeException("subscription failed, not connected"));
            return resp;
        }
        SubscribePacket subscribe = invocation.getArgument(0);

        // mocking because crt packets have private constructors
        SubAckPacket subAckPacket = mock(SubAckPacket.class);
        List<SubAckPacket.SubAckReasonCode> reasonCodes = new ArrayList<>();
        when(subAckPacket.getReasonCodes()).thenReturn(reasonCodes);

        for (SubscribePacket.Subscription subscription : subscribe.getSubscriptions()) {
            synchronized (subscriptionsLock) {
                subscriptions.add(subscribe);
            }
            reasonCodes.add(SubAckPacket.SubAckReasonCode.getEnumValueFromInteger(
                    subscription.getQOS().getValue()));
        }

        return CompletableFuture.completedFuture(subAckPacket);
    }

    private CompletableFuture<PubAckPacket> onPublish(InvocationOnMock invocation) {
        PublishPacket publish = invocation.getArgument(0);
        toPublish.add(publish);
        flush();

        // mocking because crt packets have private constructors
        PubAckPacket pubAckPacket = mock(PubAckPacket.class);
        when(pubAckPacket.getReasonCode()).thenReturn(PubAckPacket.PubAckReasonCode.getEnumValueFromInteger(
                publish.getQOS().getValue()));

        return CompletableFuture.completedFuture(pubAckPacket);
    }

    private CompletableFuture<UnsubAckPacket> onUnsubscribe(InvocationOnMock invocation) {
        if (!online.get()) {
            CompletableFuture<UnsubAckPacket> resp = new CompletableFuture<>();
            resp.completeExceptionally(new RuntimeException("unsubscribe failed, not connected"));
            return resp;
        }
        UnsubscribePacket unsubscribe = invocation.getArgument(0);

        List<UnsubAckPacket.UnsubAckReasonCode> reasonCodes = new ArrayList<>();

        UnsubAckPacket unsubAckPacket = mock(UnsubAckPacket.class);
        when(unsubAckPacket.getReasonCodes()).thenReturn(reasonCodes);

        synchronized (subscriptionsLock) {
            Iterator<SubscribePacket> iter = subscriptions.iterator();
            while (iter.hasNext()) {
                SubscribePacket packet = iter.next();
                for (String subscription : unsubscribe.getSubscriptions()) {
                    if (packet.getSubscriptions().removeIf(s -> s.getTopicFilter().equals(subscription))) {
                        reasonCodes.add(UnsubAckPacket.UnsubAckReasonCode.SUCCESS);
                    }
                }
                if (packet.getSubscriptions().isEmpty()) {
                    iter.remove();
                }
            }
        }
        return CompletableFuture.completedFuture(unsubAckPacket);
    }

    private void flush() {
        synchronized (publishLock) {
            Iterator<PublishPacket> iter = toPublish.iterator();
            while (iter.hasNext()) {
                if (!online.get()) {
                    return;
                }
                PublishPacket publish = iter.next();
                published.add(publish);
                synchronized (subscriptionsLock) {
                    for (SubscribePacket subscribe : subscriptions) {
                        for (SubscribePacket.Subscription subscription : subscribe.getSubscriptions()) {
                            if (subscription.getTopicFilter().equals(publish.getTopic())) {
                                PublishReturn publishReturn = mock(PublishReturn.class);
                                when(publishReturn.getPublishPacket()).thenReturn(publish);
                                publishEvents.onMessageReceived(client, publishReturn);
                            }
                        }
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
        onDisconnection();
    }

    private void onDisconnection() {
        DisconnectPacket disconnectPacket = mock(DisconnectPacket.class);
        lenient().when(disconnectPacket.getReasonCode()).thenReturn(DisconnectPacket.DisconnectReasonCode.NORMAL_DISCONNECTION);

        OnDisconnectionReturn onDisconnectionReturn = mock(OnDisconnectionReturn.class);
        lenient().when(onDisconnectionReturn.getDisconnectPacket()).thenReturn(disconnectPacket);

        lifecycleEvents.onDisconnection(client, onDisconnectionReturn);
    }

    private void onConnect() {
        if (cleanSession) {
            synchronized (subscriptionsLock) {
                subscriptions.clear();
            }
        }

        OnAttemptingConnectReturn onAttemptingConnectReturn = mock(OnAttemptingConnectReturn.class);
        lifecycleEvents.onAttemptingConnect(client, onAttemptingConnectReturn);

        ConnAckPacket connAckPacket = mock(ConnAckPacket.class);
        // TODO other values?
        lenient().when(connAckPacket.getReasonCode()).thenReturn(ConnAckPacket.ConnectReasonCode.SUCCESS);

        OnConnectionSuccessReturn onConnectionSuccessReturn = mock(OnConnectionSuccessReturn.class);
        lenient().when(onConnectionSuccessReturn.getConnAckPacket()).thenReturn(connAckPacket);
        lifecycleEvents.onConnectionSuccess(client, onConnectionSuccessReturn);
    }
}
