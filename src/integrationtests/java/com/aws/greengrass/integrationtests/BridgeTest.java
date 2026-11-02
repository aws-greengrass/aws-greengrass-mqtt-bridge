/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;


import com.aws.greengrass.integrationtests.extensions.BridgeIntegrationTest;
import com.aws.greengrass.integrationtests.extensions.BridgeIntegrationTestContext;
import com.aws.greengrass.integrationtests.extensions.Broker;
import com.aws.greengrass.mqtt.bridge.BridgeConfig;
import com.aws.greengrass.mqtt.bridge.MQTTBridge;
import com.aws.greengrass.mqtt.bridge.model.Mqtt5RouteOptions;
import com.aws.greengrass.mqtt.bridge.model.MqttMessage;
import com.aws.greengrass.mqttclient.v5.Publish;
import com.aws.greengrass.mqttclient.v5.Subscribe;
import com.aws.greengrass.mqttclient.v5.UserProperty;
import com.aws.greengrass.util.Pair;
import com.aws.greengrass.util.Utils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.testcommons.testutilities.TestUtils.asyncAssertOnBiConsumer;
import static com.aws.greengrass.testcommons.testutilities.TestUtils.asyncAssertOnConsumer;
import static com.github.grantwest.eventually.EventuallyLambdaMatcher.eventuallyEval;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BridgeTest {

    /**
     * Operations are between local broker and a mocked IoT Core,
     * so we expect them to happen quickly.
     */
    private static final long AWAIT_TIMEOUT_SECONDS = 2;

    @BridgeIntegrationTest(
            withConfig = "mqtt5_local_to_iotcore_nolocal.yaml",
            withBrokers = Broker.MQTT5)
    void GIVEN_mqtt5_and_mapping_between_local_and_iotcore_with_nolocal_WHEN_message_published_THEN_message_does_not_loop(BridgeIntegrationTestContext context) throws Exception {
        Pair<CompletableFuture<Void>, Consumer<Publish>> iotCoreTopicSubscription
                = asyncAssertOnConsumer(p -> {}, 0);

        context.getIotCoreClient().getIotMqttClient().subscribe(Subscribe.builder()
                .topic("topic/toIotCore")
                .callback(iotCoreTopicSubscription.getRight())
                .build()).get(5L, TimeUnit.SECONDS);

        context.getLocalV5Client().publish(
                MqttMessage.builder()
                        .topic("topic/toIotCore")
                        .payload("message".getBytes(StandardCharsets.UTF_8))
                        // mqtt5-specific fields below.
                        .userProperties(Collections.singletonList(new UserProperty("key", "val")))
                        .responseTopic("response topic")
                        .messageExpiryIntervalSeconds(1234L)
                        .payloadFormat(Publish.PayloadFormatIndicator.UTF8)
                        .contentType("contentType")
                        .build());

        assertThrows(TimeoutException.class, () -> iotCoreTopicSubscription.getLeft().get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

    @BridgeIntegrationTest(
            withConfig = "mqtt3_local_and_iotcore.yaml",
            withBrokers = {Broker.MQTT5, Broker.MQTT3})
    void GIVEN_mqtt3_and_mapping_between_local_and_iotcore_WHEN_iotcore_message_received_THEN_message_bridged_to_local(BridgeIntegrationTestContext context) throws Exception {
        MqttMessage expectedMessage = MqttMessage.builder()
                .topic("topic/toLocal")
                .payload("message".getBytes(StandardCharsets.UTF_8))
                .build();

        Pair<CompletableFuture<Void>, BiConsumer<String, org.eclipse.paho.client.mqttv3.MqttMessage>> subscribeCallback
                = asyncAssertOnBiConsumer((topic, message) -> assertEquals(expectedMessage, MqttMessage.fromPahoMQTT3(topic, message)));

        context.getLocalV3Client().getMqttClientInternal().subscribe("topic/toLocal",
                (topic, message) -> subscribeCallback.getRight().accept(topic, message));

        context.getIotCoreClient().publish(
                MqttMessage.builder()
                        .topic("topic/toLocal")
                        .payload("message".getBytes(StandardCharsets.UTF_8))
                        // mqtt5-specific fields below.
                        // this test uses mqtt3 protocol,
                        // so we expect this fields to be dropped during bridging
                        .userProperties(Collections.singletonList(new UserProperty("key", "val")))
                        .responseTopic("response topic")
                        .messageExpiryIntervalSeconds(1234L)
                        .payloadFormat(Publish.PayloadFormatIndicator.UTF8)
                        .contentType("contentType")
                        .build());

        subscribeCallback.getLeft().get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }


    @BridgeIntegrationTest(
            withConfig = "mqtt3_local_and_iotcore.yaml",
            withBrokers = {Broker.MQTT5})
    void GIVEN_mqtt3_and_mapping_between_local_and_iotcore_WHEN_local_message_received_THEN_message_bridged_to_iotcore(BridgeIntegrationTestContext context) throws Exception {
        MqttMessage expectedMessage = MqttMessage.builder()
                .topic("topic/toIotCore")
                .payload("message".getBytes(StandardCharsets.UTF_8))
                .build();

        Pair<CompletableFuture<Void>, Consumer<Publish>> subscribeCallback
                = asyncAssertOnConsumer(p -> assertEquals(expectedMessage, MqttMessage.fromSpoolerV5Model(p)));

        context.getIotCoreClient().getIotMqttClient().subscribe(Subscribe.builder()
                .topic("topic/toIotCore")
                .callback(subscribeCallback.getRight())
                .build());

        context.getLocalV3Client().publish(
                MqttMessage.builder()
                        .topic("topic/toIotCore")
                        .payload("message".getBytes(StandardCharsets.UTF_8))
                        // mqtt5-specific fields below.
                        // will be ignored by v3 mqtt client
                        .userProperties(Collections.singletonList(new UserProperty("key", "val")))
                        .responseTopic("response topic")
                        .messageExpiryIntervalSeconds(1234L)
                        .payloadFormat(Publish.PayloadFormatIndicator.UTF8)
                        .contentType("contentType")
                        .build());

        subscribeCallback.getLeft().get(5L, TimeUnit.SECONDS);
    }

    @BridgeIntegrationTest(
            withConfig = "mqtt5_local_and_iotcore.yaml",
            withBrokers = Broker.MQTT5)
    void GIVEN_mqtt5_and_mapping_between_local_and_iotcore_WHEN_local_message_received_THEN_message_bridged_to_iotcore(BridgeIntegrationTestContext context) throws Exception {
        MqttMessage expectedMessage = MqttMessage.builder()
                .topic("topic/toIotCore")
                .payload("message".getBytes(StandardCharsets.UTF_8))
                // mqtt5-specific fields below.
                .userProperties(Collections.singletonList(new UserProperty("key", "val")))
                .responseTopic("response topic")
                .messageExpiryIntervalSeconds(1234L)
                .payloadFormat(Publish.PayloadFormatIndicator.UTF8)
                .contentType("contentType")
                .build();

        Pair<CompletableFuture<Void>, Consumer<Publish>> subscribeCallback
                = asyncAssertOnConsumer(p -> assertEquals(expectedMessage, MqttMessage.fromSpoolerV5Model(p)));

        context.getIotCoreClient().getIotMqttClient().subscribe(Subscribe.builder()
                .topic("topic/toIotCore")
                .callback(subscribeCallback.getRight())
                .build());

        context.getLocalV5Client().publish(
                MqttMessage.builder()
                        .topic("topic/toIotCore")
                        .payload("message".getBytes(StandardCharsets.UTF_8))
                        // mqtt5-specific fields below.
                        .userProperties(Collections.singletonList(new UserProperty("key", "val")))
                        .responseTopic("response topic")
                        .messageExpiryIntervalSeconds(1234L)
                        .payloadFormat(Publish.PayloadFormatIndicator.UTF8)
                        .contentType("contentType")
                        .build());

        subscribeCallback.getLeft().get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @BridgeIntegrationTest(
            withConfig = "mqtt5_local_to_iotcore_retain_as_published.yaml",
            withBrokers = Broker.MQTT5)
    void GIVEN_mqtt5_and_local_mapping_with_retainAsPublished_WHEN_message_published_with_retain_THEN_message_bridged_with_retain_flag(BridgeIntegrationTestContext context)
            throws Exception {
        String topic = "topic/toIotCore";
        Set<String> topics = new HashSet<>();
        topics.add(topic);

        MqttMessage expectedMessage = MqttMessage.builder()
                .topic(topic)
                .payload("message".getBytes(StandardCharsets.UTF_8))
                // mqtt5-specific fields below.
                .userProperties(Collections.singletonList(new UserProperty("key", "val")))
                .responseTopic("response topic")
                .messageExpiryIntervalSeconds(1234L)
                .payloadFormat(Publish.PayloadFormatIndicator.UTF8)
                .contentType("contentType")
                .retain(true)
                .build();

        Pair<CompletableFuture<Void>, Consumer<Publish>> publishHandler = asyncAssertOnConsumer(p -> {
           MqttMessage msg = MqttMessage.fromSpoolerV5Model(p);
           assertEquals(Arrays.toString(expectedMessage.getPayload()), Arrays.toString(msg.getPayload()));
           assertEquals(expectedMessage.isRetain(), msg.isRetain());
        });

        context.getIotCoreClient().getIotMqttClient().subscribe(Subscribe.builder()
                .topic(topic)
                .callback(publishHandler.getRight())
                .build());

        context.getLocalV5Client().publish(
                MqttMessage.builder()
                        .topic(topic)
                        .payload("message".getBytes(StandardCharsets.UTF_8))
                        // mqtt5-specific fields below.
                        .userProperties(Collections.singletonList(new UserProperty("key", "val")))
                        .responseTopic("response topic")
                        .messageExpiryIntervalSeconds(1234L)
                        .payloadFormat(Publish.PayloadFormatIndicator.UTF8)
                        .contentType("contentType")
                        .retain(true)
                        .build());

        publishHandler.getLeft().get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        Publish msg = context.getMockMqttClient().getPublished().get(0);
        assertEquals(expectedMessage.isRetain(), msg.isRetain());
    }

    @BridgeIntegrationTest(
            withConfig = "mqtt5_local_to_iotcore_retain_as_published.yaml",
            withBrokers = Broker.MQTT5)
    void GIVEN_mqtt5_and_local_mapping_with_retainAsPublished_WHEN_message_published_without_retain_THEN_message_bridged_without_retain_flag(BridgeIntegrationTestContext context)
            throws Exception {
        String topic = "topic/toIotCore";
        Set<String> topics = new HashSet<>();
        topics.add(topic);

        MqttMessage expectedMessage = MqttMessage.builder()
                .topic(topic)
                .payload("message".getBytes(StandardCharsets.UTF_8))
                // mqtt5-specific fields below.
                .userProperties(Collections.singletonList(new UserProperty("key", "val")))
                .responseTopic("response topic")
                .messageExpiryIntervalSeconds(1234L)
                .payloadFormat(Publish.PayloadFormatIndicator.UTF8)
                .contentType("contentType")
                .retain(false)
                .build();

        Pair<CompletableFuture<Void>, Consumer<Publish>> publishHandler = asyncAssertOnConsumer(p -> {
            MqttMessage msg = MqttMessage.fromSpoolerV5Model(p);
            assertEquals(Arrays.toString(expectedMessage.getPayload()), Arrays.toString(msg.getPayload()));
            assertEquals(expectedMessage.isRetain(), msg.isRetain());
        });

        context.getIotCoreClient().getIotMqttClient().subscribe(Subscribe.builder()
                .topic(topic)
                .callback(publishHandler.getRight())
                .build());

        context.getLocalV5Client().publish(
                MqttMessage.builder()
                        .topic(topic)
                        .payload("message".getBytes(StandardCharsets.UTF_8))
                        // mqtt5-specific fields below.
                        .userProperties(Collections.singletonList(new UserProperty("key", "val")))
                        .responseTopic("response topic")
                        .messageExpiryIntervalSeconds(1234L)
                        .payloadFormat(Publish.PayloadFormatIndicator.UTF8)
                        .contentType("contentType")
                        .build());

        publishHandler.getLeft().get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        Publish msg = context.getMockMqttClient().getPublished().get(0);
        assertEquals(expectedMessage.isRetain(), msg.isRetain());
    }

    @BridgeIntegrationTest(
            withConfig = "mqtt5_local_and_iotcore.yaml",
            withBrokers = Broker.MQTT5)
    void GIVEN_mqtt5_and_local_mapping_without_retainAsPublished_WHEN_message_published_with_retain_THEN_message_bridged_without_retain_flag(BridgeIntegrationTestContext context)
            throws Exception {
        String topic = "topic/toIotCore";
        Set<String> topics = new HashSet<>();
        topics.add(topic);

        MqttMessage expectedMessage = MqttMessage.builder()
                .topic(topic)
                .payload("message".getBytes(StandardCharsets.UTF_8))
                // mqtt5-specific fields below.
                .userProperties(Collections.singletonList(new UserProperty("key", "val")))
                .responseTopic("response topic")
                .messageExpiryIntervalSeconds(1234L)
                .payloadFormat(Publish.PayloadFormatIndicator.UTF8)
                .contentType("contentType")
                .retain(false)
                .build();

        Pair<CompletableFuture<Void>, Consumer<Publish>> publishHandler = asyncAssertOnConsumer(p -> {
            MqttMessage msg = MqttMessage.fromSpoolerV5Model(p);
            assertEquals(Arrays.toString(expectedMessage.getPayload()), Arrays.toString(msg.getPayload()));
            assertEquals(expectedMessage.isRetain(), msg.isRetain());
        });

        context.getIotCoreClient().getIotMqttClient().subscribe(Subscribe.builder()
                .topic(topic)
                .callback(publishHandler.getRight())
                .build());

        context.getLocalV5Client().publish(
                MqttMessage.builder()
                        .topic(topic)
                        .payload("message".getBytes(StandardCharsets.UTF_8))
                        // mqtt5-specific fields below.
                        .userProperties(Collections.singletonList(new UserProperty("key", "val")))
                        .responseTopic("response topic")
                        .messageExpiryIntervalSeconds(1234L)
                        .payloadFormat(Publish.PayloadFormatIndicator.UTF8)
                        .contentType("contentType")
                        .retain(true)
                        .build());

        publishHandler.getLeft().get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        Publish msg = context.getMockMqttClient().getPublished().get(0);
        assertEquals(expectedMessage.isRetain(), msg.isRetain());
    }

    @BridgeIntegrationTest(
            withConfig = "mqtt5_local_to_iotcore_retain_as_published_wildcard_and_prefix.yaml",
            withBrokers = Broker.MQTT5)
    void GIVEN_mqtt5_and_wildcard_local_mapping_with_retainAsPublished_WHEN_message_published_with_retain_THEN_message_bridged_with_retain_flag(BridgeIntegrationTestContext context)
            throws Exception {
        assertRetainedLocalMessageBridgedToIotCore(context, "wildcard/sensor1/toIotCore",
                "wildcard/sensor1/toIotCore");
    }

    @BridgeIntegrationTest(
            withConfig = "mqtt5_local_to_iotcore_retain_as_published_wildcard_and_prefix.yaml",
            withBrokers = Broker.MQTT5)
    void GIVEN_mqtt5_and_prefixed_local_mapping_with_retainAsPublished_WHEN_message_published_with_retain_THEN_message_bridged_with_retain_flag(BridgeIntegrationTestContext context)
            throws Exception {
        assertRetainedLocalMessageBridgedToIotCore(context, "prefixed/toIotCore", "site1/prefixed/toIotCore");
    }

    @BridgeIntegrationTest(
            withConfig = "mqtt5_local_to_iotcore_retain_as_published_shared_filter.yaml",
            withBrokers = Broker.MQTT5)
    void GIVEN_mqtt5_and_local_mappings_sharing_filter_WHEN_message_published_with_retain_THEN_each_route_uses_own_retainAsPublished(BridgeIntegrationTestContext context)
            throws Exception {
        Map<String, Boolean> retainByTopic = publishRetainedLocalMessageAndGetIotCoreRetainByTopic(context,
                "shared/toIotCore", "shared/toIotCore", "backup/shared/toIotCore");
        assertEquals(Utils.immutableMap("shared/toIotCore", true, "backup/shared/toIotCore", false), retainByTopic);
    }

    @BridgeIntegrationTest(
            withConfig = "mqtt5_local_to_iotcore_retain_as_published_shared_filter.yaml",
            withBrokers = Broker.MQTT5)
    void GIVEN_mqtt5_and_local_mappings_WHEN_mqtt5RouteOptions_updated_THEN_message_bridged_with_updated_retainAsPublished(BridgeIntegrationTestContext context)
            throws Exception {
        // move retainAsPublished from plainToIotCore to backupToIotCore, without changing the mapping
        Map<String, Object> updatedRouteOptions =
                Utils.immutableMap("backupToIotCore", Utils.immutableMap("retainAsPublished", "true"));
        context.getKernel().locate(MQTTBridge.SERVICE_NAME).getConfig()
                .lookupTopics(CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_MQTT_5_ROUTE_OPTIONS)
                .replaceAndWait(updatedRouteOptions);
        assertThat("route options config update", () -> context.getConfig().getMqtt5RouteOptions(),
                eventuallyEval(is(Utils.immutableMap("backupToIotCore",
                        Mqtt5RouteOptions.builder().retainAsPublished(true).build()))));

        Map<String, Boolean> retainByTopic = publishRetainedLocalMessageAndGetIotCoreRetainByTopic(context,
                "shared/toIotCore", "shared/toIotCore", "backup/shared/toIotCore");
        assertEquals(Utils.immutableMap("shared/toIotCore", false, "backup/shared/toIotCore", true), retainByTopic);
    }

    private static Map<String, Boolean> publishRetainedLocalMessageAndGetIotCoreRetainByTopic(
            BridgeIntegrationTestContext context, String localTopic, String... iotCoreTopics) throws Exception {
        List<CompletableFuture<Void>> received = new ArrayList<>();
        for (String iotCoreTopic : iotCoreTopics) {
            Pair<CompletableFuture<Void>, Consumer<Publish>> handler = asyncAssertOnConsumer(p -> {});
            received.add(handler.getLeft());
            context.getIotCoreClient().getIotMqttClient().subscribe(Subscribe.builder()
                    .topic(iotCoreTopic)
                    .callback(handler.getRight())
                    .build());
        }

        context.getLocalV5Client().publish(
                MqttMessage.builder()
                        .topic(localTopic)
                        .payload("message".getBytes(StandardCharsets.UTF_8))
                        .retain(true)
                        .build());

        for (CompletableFuture<Void> future : received) {
            future.get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        return context.getMockMqttClient().getPublished().stream()
                .collect(Collectors.toMap(Publish::getTopic, Publish::isRetain));
    }

    private static void assertRetainedLocalMessageBridgedToIotCore(BridgeIntegrationTestContext context,
                                                                   String localTopic, String iotCoreTopic)
            throws Exception {
        Pair<CompletableFuture<Void>, Consumer<Publish>> publishHandler = asyncAssertOnConsumer(p -> {
            MqttMessage msg = MqttMessage.fromSpoolerV5Model(p);
            assertEquals("message", new String(msg.getPayload(), StandardCharsets.UTF_8));
            assertTrue(msg.isRetain());
        });

        context.getIotCoreClient().getIotMqttClient().subscribe(Subscribe.builder()
                .topic(iotCoreTopic)
                .callback(publishHandler.getRight())
                .build());

        context.getLocalV5Client().publish(
                MqttMessage.builder()
                        .topic(localTopic)
                        .payload("message".getBytes(StandardCharsets.UTF_8))
                        .retain(true)
                        .build());

        publishHandler.getLeft().get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        Publish msg = context.getMockMqttClient().getPublished().get(0);
        assertEquals(iotCoreTopic, msg.getTopic());
        assertTrue(msg.isRetain());
    }
}
