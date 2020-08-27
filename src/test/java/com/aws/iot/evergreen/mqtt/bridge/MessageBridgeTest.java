/* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0 */

package com.aws.iot.evergreen.mqtt.bridge;

import com.aws.iot.evergreen.testcommons.testutilities.EGExtension;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@ExtendWith({MockitoExtension.class, EGExtension.class})
public class MessageBridgeTest {
    @Test
    void GIVEN_message_bridge_WHEN_listener_added_THEN_notified_on_message() throws InterruptedException {
        CountDownLatch listenerRan = new CountDownLatch(1);
        TopicMapping.TopicType sourceType = TopicMapping.TopicType.IotCore;

        MessageBridge.MessageListener listener = (sourceType1, msg) -> {
            MatcherAssert.assertThat(sourceType1, Matchers.is(Matchers.equalTo(sourceType)));
            listenerRan.countDown();
        };
        MessageBridge messageBridge = new MessageBridge();
        messageBridge.addListener(listener, sourceType);

        messageBridge.notifyMessage(new Message("bla", "".getBytes()), sourceType);
        Assertions.assertTrue(listenerRan.await(1L, TimeUnit.SECONDS));
    }

    @Test
    void GIVEN_message_bridge_WHEN_multiple_listeners_added_of_same_type_THEN_notified_on_message()
            throws InterruptedException {
        CountDownLatch listenerRan = new CountDownLatch(2);
        TopicMapping.TopicType sourceType = TopicMapping.TopicType.IotCore;

        MessageBridge.MessageListener listener1 = (sourceType1, msg) -> {
            MatcherAssert.assertThat(sourceType1, Matchers.is(Matchers.equalTo(sourceType)));
            listenerRan.countDown();
        };

        MessageBridge.MessageListener listener2 = (sourceType1, msg) -> {
            MatcherAssert.assertThat(sourceType1, Matchers.is(Matchers.equalTo(sourceType)));
            listenerRan.countDown();
        };

        MessageBridge messageBridge = new MessageBridge();
        messageBridge.addListener(listener1, sourceType);
        messageBridge.addListener(listener2, sourceType);

        messageBridge.notifyMessage(new Message("bla", "".getBytes()), TopicMapping.TopicType.IotCore);
        Assertions.assertTrue(listenerRan.await(1L, TimeUnit.SECONDS));
    }

    @Test
    void GIVEN_message_bridge_WHEN_multiple_listeners_added_of_different_type_THEN_notified_on_message()
            throws InterruptedException {
        CountDownLatch listenerRan = new CountDownLatch(2);

        MessageBridge.MessageListener listener1 = (sourceType1, msg) -> {
            MatcherAssert.assertThat(sourceType1, Matchers.is(Matchers.equalTo(TopicMapping.TopicType.IotCore)));
            listenerRan.countDown();
        };

        MessageBridge.MessageListener listener2 = (sourceType1, msg) -> {
            MatcherAssert.assertThat(sourceType1, Matchers.is(Matchers.equalTo(TopicMapping.TopicType.LocalMqtt)));
            listenerRan.countDown();
        };

        MessageBridge messageBridge = new MessageBridge();
        messageBridge.addListener(listener1, TopicMapping.TopicType.IotCore);
        messageBridge.addListener(listener2, TopicMapping.TopicType.LocalMqtt);

        messageBridge.notifyMessage(new Message("bla", "".getBytes()), TopicMapping.TopicType.IotCore);
        messageBridge.notifyMessage(new Message("bla", "".getBytes()), TopicMapping.TopicType.LocalMqtt);
        Assertions.assertTrue(listenerRan.await(1L, TimeUnit.SECONDS));
    }

    @Test
    void GIVEN_message_bridge_WHEN_listener_added_and_removed_THEN_stop_notifying_on_message()
            throws InterruptedException {
        CountDownLatch listenerRanOnce = new CountDownLatch(1);
        CountDownLatch listenerRanAgain = new CountDownLatch(2);

        MessageBridge.MessageListener listener1 = (sourceType1, msg) -> {
            MatcherAssert.assertThat(sourceType1, Matchers.is(Matchers.equalTo(TopicMapping.TopicType.IotCore)));
            listenerRanOnce.countDown();
            listenerRanAgain.countDown();
        };

        MessageBridge messageBridge = new MessageBridge();
        messageBridge.addListener(listener1, TopicMapping.TopicType.IotCore);

        messageBridge.notifyMessage(new Message("bla", "".getBytes()), TopicMapping.TopicType.IotCore);
        Assertions.assertTrue(listenerRanOnce.await(1L, TimeUnit.SECONDS));

        messageBridge.removeListener(listener1, TopicMapping.TopicType.IotCore);

        messageBridge.notifyMessage(new Message("bla", "".getBytes()), TopicMapping.TopicType.LocalMqtt);
        Assertions.assertFalse(listenerRanAgain.await(1L, TimeUnit.SECONDS));
    }
}
