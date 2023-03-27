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
import software.amazon.awssdk.crt.mqtt5.OnConnectionFailureReturn;
import software.amazon.awssdk.crt.mqtt5.OnConnectionSuccessReturn;
import software.amazon.awssdk.crt.mqtt5.OnDisconnectionReturn;
import software.amazon.awssdk.crt.mqtt5.PublishResult;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class MockMqtt5Client {

    final NextReasonCode<SubAckPacket.SubAckReasonCode> nextSubAckReasonCode = new NextReasonCode<>();
    final NextReasonCode<PubAckPacket.PubAckReasonCode> nextPubAckReasonCode = new NextReasonCode<>();
    final NextReasonCode<UnsubAckPacket.UnsubAckReasonCode> nextUnsubAckReasonCode = new NextReasonCode<>();
    final NextReasonCode<ConnAckPacket.ConnectReasonCode> nextConnectReasonCode = new NextReasonCode<>();

    volatile boolean throwExceptions = false;

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
            offline(DisconnectPacket.DisconnectReasonCode.NORMAL_DISCONNECTION);
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
        if (throwExceptions) {
            CompletableFuture<SubAckPacket> resp = new CompletableFuture<>();
            resp.completeExceptionally(new RuntimeException("subscription failed, simulated exception"));
            return resp;
        }

        SubscribePacket subscribe = invocation.getArgument(0);

        // mocking because crt packets have private constructors
        SubAckPacket subAckPacket = mock(SubAckPacket.class);
        List<SubAckPacket.SubAckReasonCode> reasonCodes = new ArrayList<>();
        when(subAckPacket.getReasonCodes()).thenReturn(reasonCodes);

        for (SubscribePacket.Subscription subscription : subscribe.getSubscriptions()) {
            SubAckPacket.SubAckReasonCode reasonCode = nextSubAckReasonCode.getNext(subscription.getTopicFilter());
            if (reasonCode == null) {
                reasonCode = SubAckPacket.SubAckReasonCode.getEnumValueFromInteger(
                        subscription.getQOS().getValue());
            }
            switch (reasonCode) {
                case GRANTED_QOS_0:
                case GRANTED_QOS_1:
                case GRANTED_QOS_2:
                    synchronized (subscriptionsLock) {
                        subscriptions.add(subscribe);
                    }
                    break;
                default:
                    break;
            }
            reasonCodes.add(reasonCode);
        }

        return CompletableFuture.completedFuture(subAckPacket);
    }

    private CompletableFuture<PublishResult> onPublish(InvocationOnMock invocation) {
        if (throwExceptions) {
            CompletableFuture<PublishResult> resp = new CompletableFuture<>();
            resp.completeExceptionally(new RuntimeException("publish failed, simulated exception"));
            return resp;
        }

        PublishPacket publish = invocation.getArgument(0);

        // mocking because crt packets have private constructors
        PubAckPacket pubAckPacket = mock(PubAckPacket.class);
        PubAckPacket.PubAckReasonCode reasonCode = nextPubAckReasonCode.getNext(publish.getTopic());
        if (reasonCode == null) {
            reasonCode = PubAckPacket.PubAckReasonCode.SUCCESS;
        }
        lenient().when(pubAckPacket.getReasonCode()).thenReturn(reasonCode);

        if (reasonCode == PubAckPacket.PubAckReasonCode.SUCCESS) {
            toPublish.add(publish);
        }
        flush();

        PublishResult publishResult = mock(PublishResult.class);
        lenient().when(publishResult.getResultPubAck()).thenReturn(pubAckPacket);

        return CompletableFuture.completedFuture(publishResult);
    }

    private CompletableFuture<UnsubAckPacket> onUnsubscribe(InvocationOnMock invocation) {
        if (!online.get()) {
            CompletableFuture<UnsubAckPacket> resp = new CompletableFuture<>();
            resp.completeExceptionally(new RuntimeException("unsubscribe failed, not connected"));
            return resp;
        }
        if (throwExceptions) {
            CompletableFuture<UnsubAckPacket> resp = new CompletableFuture<>();
            resp.completeExceptionally(new RuntimeException("unsubscribe failed, simulated exception"));
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
                for (String toUnsubscribe : unsubscribe.getSubscriptions()) {
                    Iterator<SubscribePacket.Subscription> subIter = packet.getSubscriptions().iterator();
                    while (subIter.hasNext()) {
                        SubscribePacket.Subscription subscription = subIter.next();
                        if (!toUnsubscribe.equals(subscription.getTopicFilter())) {
                            continue;
                        }
                        UnsubAckPacket.UnsubAckReasonCode reasonCode = nextUnsubAckReasonCode.getNext(toUnsubscribe);
                        if (reasonCode == null) {
                            reasonCode = UnsubAckPacket.UnsubAckReasonCode.SUCCESS;
                        }
                        reasonCodes.add(reasonCode);
                        if (reasonCode == UnsubAckPacket.UnsubAckReasonCode.SUCCESS) {
                            subIter.remove();
                        }
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

    public void offline(DisconnectPacket.DisconnectReasonCode reasonCode) {
        online.set(false);
        onDisconnection(reasonCode);
    }

    private void onDisconnection(DisconnectPacket.DisconnectReasonCode reasonCode) {
        DisconnectPacket disconnectPacket = mock(DisconnectPacket.class);
        lenient().when(disconnectPacket.getReasonCode()).thenReturn(reasonCode);

        OnDisconnectionReturn onDisconnectionReturn = mock(OnDisconnectionReturn.class);
        lenient().when(onDisconnectionReturn.getDisconnectPacket()).thenReturn(disconnectPacket);
        lenient().when(onDisconnectionReturn.getErrorCode()).thenReturn(reasonCode.getValue());

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

        ConnAckPacket.ConnectReasonCode reasonCode = nextConnectReasonCode.getNext();
        if (reasonCode == null) {
            reasonCode = ConnAckPacket.ConnectReasonCode.SUCCESS;
        }
        ConnAckPacket connAckPacket = mock(ConnAckPacket.class);
        lenient().when(connAckPacket.getReasonCode()).thenReturn(reasonCode);

        if (reasonCode == ConnAckPacket.ConnectReasonCode.SUCCESS) {
            OnConnectionSuccessReturn onConnectionSuccessReturn = mock(OnConnectionSuccessReturn.class);
            lenient().when(onConnectionSuccessReturn.getConnAckPacket()).thenReturn(connAckPacket);
            lifecycleEvents.onConnectionSuccess(client, onConnectionSuccessReturn);
        } else {
            OnConnectionFailureReturn onConnectionFailureReturn = mock(OnConnectionFailureReturn.class);
            lenient().when(onConnectionFailureReturn.getConnAckPacket()).thenReturn(connAckPacket);
            lifecycleEvents.onConnectionFailure(client, onConnectionFailureReturn);
        }
    }

    static class NextReasonCode<T> {

        private static final String NO_TOPIC = "";

        private final Map<String, Queue<T>> reasonCodesByTopic = new HashMap<>();

        public T getNext() {
            return getNext(NO_TOPIC);
        }

        public T getNext(String topic) {
            Queue<T> reasonCodes = reasonCodesByTopic.get(topic);
            if (reasonCodes != null) {
                return reasonCodes.poll();
            }
            return null;
        }

        public void add(T reasonCode) {
            add(NO_TOPIC, reasonCode);
        }

        public void add(String topic, T reasonCode) {
            reasonCodesByTopic.computeIfAbsent(topic, k -> new LinkedList<>()).add(reasonCode);
        }
    }
}
