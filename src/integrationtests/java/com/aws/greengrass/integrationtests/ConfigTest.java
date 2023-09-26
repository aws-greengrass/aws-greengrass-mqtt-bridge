/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.config.Topic;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.config.UpdateBehaviorTree;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.integrationtests.extensions.BridgeIntegrationTest;
import com.aws.greengrass.integrationtests.extensions.BridgeIntegrationTestContext;
import com.aws.greengrass.integrationtests.extensions.Broker;
import com.aws.greengrass.integrationtests.extensions.TestWithMqtt3Broker;
import com.aws.greengrass.integrationtests.extensions.TestWithMqtt5Broker;
import com.aws.greengrass.integrationtests.extensions.WithKernel;
import com.aws.greengrass.lifecyclemanager.GlobalStateChangeListener;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import com.aws.greengrass.mqtt.bridge.BridgeConfig;
import com.aws.greengrass.mqtt.bridge.MQTTBridge;
import com.aws.greengrass.mqtt.bridge.TopicMapping;
import com.aws.greengrass.mqtt.bridge.clients.MQTTClient;
import com.aws.greengrass.mqtt.bridge.model.BridgeConfigReference;
import com.aws.greengrass.mqtt.bridge.model.Mqtt5RouteOptions;
import com.aws.greengrass.mqtt.bridge.model.MqttMessage;
import com.aws.greengrass.mqttclient.v5.Publish;
import com.aws.greengrass.mqttclient.v5.UserProperty;
import com.aws.greengrass.util.Pair;
import com.aws.greengrass.util.Utils;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.github.grantwest.eventually.EventuallyLambdaMatcher.eventuallyEval;
import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static com.aws.greengrass.testcommons.testutilities.TestUtils.asyncAssertOnConsumer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@BridgeIntegrationTest
public class ConfigTest {
    private static final long AWAIT_TIMEOUT_SECONDS = 30L;
    private static final long RECEIVE_PUBLISH_SECONDS = 2L;
    private static final Supplier<UpdateBehaviorTree> MERGE_UPDATE_BEHAVIOR =
            () -> new UpdateBehaviorTree(UpdateBehaviorTree.UpdateBehavior.MERGE, System.currentTimeMillis());

    BridgeIntegrationTestContext testContext;

    @TestWithMqtt5Broker
    @WithKernel("mqtt5_config_ssl.yaml")
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_multiple_config_changes_consecutively_THEN_bridge_reinstalls_once(Broker broker, ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, InterruptedException.class);

        CountDownLatch bridgeRestarted = new CountDownLatch(1);
        AtomicInteger numRestarts = new AtomicInteger();

        testContext.getKernel().getContext().addGlobalStateChangeListener((GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(MQTTBridge.SERVICE_NAME) && newState.equals(State.NEW)) {
                numRestarts.incrementAndGet();
                bridgeRestarted.countDown();
            }
        });

        Topics config = testContext.getKernel().locate(MQTTBridge.SERVICE_NAME).getConfig()
                .lookupTopics(CONFIGURATION_CONFIG_KEY);

        config.updateFromMap(Utils.immutableMap(BridgeConfig.KEY_CLIENT_ID, "new_client_id"), MERGE_UPDATE_BEHAVIOR.get());
        config.updateFromMap(Utils.immutableMap(BridgeConfig.KEY_BROKER_URI, String.format("tcp://localhost:%d", testContext.getBrokerTCPPort())), MERGE_UPDATE_BEHAVIOR.get());

        assertTrue(bridgeRestarted.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertEquals(1, numRestarts.get());
    }

    @TestWithMqtt5Broker
    @WithKernel("mqtt5_connect_options.yaml")
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_connect_options_set_in_config_THEN_local_client_uses_configured_values(Broker broker, ExtensionContext context)
            throws Exception {
        long expectedSessionExpiryInterval = 10;
        long expectedMaximumPacketSize = 100;
        int expectedReceiveMaximum = 1000;

        // verify that the config is correctly read
        assertEquals(expectedSessionExpiryInterval, testContext.getConfig().getSessionExpiryInterval());
        assertEquals(expectedMaximumPacketSize, testContext.getConfig().getMaximumPacketSize());
        assertEquals(expectedReceiveMaximum, testContext.getConfig().getReceiveMaximum());

        // verify that the mqtt config values are correctly set in the local client
        assertEquals(expectedSessionExpiryInterval, testContext.getLocalV5Client().getConfig().getSessionExpiryInterval());
        assertEquals(expectedMaximumPacketSize, testContext.getLocalV5Client().getConfig().getMaximumPacketSize());
        assertEquals(expectedReceiveMaximum, testContext.getLocalV5Client().getConfig().getReceiveMaximum());

        Topics config = testContext.getKernel().locate(MQTTBridge.SERVICE_NAME).getConfig()
                .lookupTopics(CONFIGURATION_CONFIG_KEY).lookupTopics("mqtt");

        // publish a small message and verify that it is received
        String topic = "topic/toLocal";
        Set<String> topics = new HashSet<>();
        topics.add(topic);

        MqttMessage expectedMessage = MqttMessage.builder()
                .topic(topic)
                .payload("abc".getBytes(StandardCharsets.UTF_8))
                .userProperties(Collections.singletonList(new UserProperty("key", "val")))
                .responseTopic("response topic")
                .messageExpiryIntervalSeconds(1234L)
                .payloadFormat(Publish.PayloadFormatIndicator.UTF8)
                .contentType("contentType")
                .build();

        Pair<CompletableFuture<Void>, Consumer<MqttMessage>> messageHandler =
                asyncAssertOnConsumer(message -> assertEquals(Arrays.toString(expectedMessage.getPayload()),
                        Arrays.toString(message.getPayload())));

        testContext.getLocalV5Client().updateSubscriptions(topics, messageHandler.getRight());
        testContext.getLocalV5Client().publish(
                MqttMessage.builder()
                        .topic(topic)
                        .payload("abc".getBytes(StandardCharsets.UTF_8))
                        .userProperties(Collections.singletonList(new UserProperty("key", "val")))
                        .responseTopic("response topic")
                        .messageExpiryIntervalSeconds(1234L)
                        .payloadFormat(Publish.PayloadFormatIndicator.UTF8)
                        .contentType("contentType")
                        .build());
        messageHandler.getLeft().get(RECEIVE_PUBLISH_SECONDS, TimeUnit.SECONDS);

        // change config values
        config.lookup(BridgeConfig.KEY_SESSION_EXPIRY_INTERVAL).withValue(1);
        config.lookup(BridgeConfig.KEY_MAXIMUM_PACKET_SIZE).withValue(1);
        config.lookup(BridgeConfig.KEY_RECEIVE_MAXIMUM).withValue(1);

        // verify that config changes take effect
        assertThat("session expiry interval config update",
                () -> testContext.getLocalV5Client().getConfig().getSessionExpiryInterval(), eventuallyEval(is(1L)));
        assertThat("maximum packet size config update", () -> testContext.getLocalV5Client().getConfig().getMaximumPacketSize(),
                eventuallyEval(is(1L)));
        assertThat("receive maximum config update", () -> testContext.getLocalV5Client().getConfig().getReceiveMaximum(),
                eventuallyEval(is(1)));

        // Publish a large message to the local broker and verify that it is not received due to the local client's
        // config
        Pair<CompletableFuture<Void>, Consumer<MqttMessage>> largeMessageHandler =
                asyncAssertOnConsumer(Assertions::assertNull);
        testContext.getLocalV5Client().updateSubscriptions(topics, largeMessageHandler.getRight());
        testContext.getLocalV5Client().publish(
                MqttMessage.builder()
                        .topic(topic)
                        .payload("this message is too large to be published".getBytes(StandardCharsets.UTF_8))
                        .userProperties(Collections.singletonList(new UserProperty("key", "val")))
                        .responseTopic("response topic")
                        .messageExpiryIntervalSeconds(1234L)
                        .payloadFormat(Publish.PayloadFormatIndicator.UTF8)
                        .contentType("contentType")
                        .build());
        assertThrows(TimeoutException.class,
                () -> largeMessageHandler.getLeft().get(RECEIVE_PUBLISH_SECONDS, TimeUnit.SECONDS));
    }

    @TestWithMqtt5Broker
    @WithKernel("mqtt5_local_to_iotcore_retain_as_published.yaml")
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_valid_mqtt5RouteOptions_updated_THEN_mapping_updated_and_bridge_restarts
            (Broker broker, ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, MqttException.class);

        Topics routeOptions =
                testContext.getKernel().locate(MQTTBridge.SERVICE_NAME).getConfig().lookupTopics(
                                CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_MQTT_5_ROUTE_OPTIONS)
                        .lookupTopics("toIotCore");
        Topic retainAsPublished = routeOptions.lookup("retainAsPublished");
        assertTrue((boolean) retainAsPublished.getOnce());

        CountDownLatch bridgeRestarted = new CountDownLatch(1);
        testContext.getKernel().getContext().addGlobalStateChangeListener((
                GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(MQTTBridge.SERVICE_NAME) && newState.equals(State.NEW)) {
                bridgeRestarted.countDown();
            }
        });

        testContext.getKernel().locate(MQTTBridge.SERVICE_NAME).getConfig().lookupTopics(CONFIGURATION_CONFIG_KEY,
                        BridgeConfig.KEY_MQTT_5_ROUTE_OPTIONS).lookupTopics("toIotCore").lookup("retainAsPublished")
                .withValue(false);
        testContext.getKernel().getContext().waitForPublishQueueToClear();
        retainAsPublished = routeOptions.lookup("retainAsPublished");
        assertFalse((boolean) retainAsPublished.getOnce());
        assertTrue(bridgeRestarted.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

    @TestWithMqtt3Broker
    @WithKernel("config.yaml")
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_multiple_serialized_config_changes_occur_THEN_bridge_reinstalls_multiple_times(Broker broker, ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, InterruptedException.class);

        Semaphore bridgeRestarted = new Semaphore(1);
        bridgeRestarted.acquire();

        testContext.getKernel().getContext().addGlobalStateChangeListener((GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(MQTTBridge.SERVICE_NAME) && newState.equals(State.RUNNING)) {
                bridgeRestarted.release();
            }
        });

        Topics config = testContext.getKernel().locate(MQTTBridge.SERVICE_NAME).getConfig()
                .lookupTopics(CONFIGURATION_CONFIG_KEY);

        int numRestarts = 5;
        for (int i = 0; i < numRestarts; i++) {
            // change the configuration and wait for bridge to restart
            config.updateFromMap(Utils.immutableMap(BridgeConfig.KEY_CLIENT_ID, String.format("clientId%d", i)), MERGE_UPDATE_BEHAVIOR.get());
            assertTrue(bridgeRestarted.tryAcquire(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        }
    }

    @TestWithMqtt3Broker
    @WithKernel("config.yaml")
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_clientId_config_changes_THEN_bridge_reinstalls(Broker broker) throws Exception {
        CountDownLatch bridgeRestarted = new CountDownLatch(1);
        testContext.getKernel().getContext().addGlobalStateChangeListener((GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(MQTTBridge.SERVICE_NAME) && newState.equals(State.NEW)) {
                bridgeRestarted.countDown();
            }
        });

        Topics config = testContext.getKernel().locate(MQTTBridge.SERVICE_NAME).getConfig()
                .lookupTopics(CONFIGURATION_CONFIG_KEY);
        config.updateFromMap(Utils.immutableMap(BridgeConfig.KEY_CLIENT_ID, "new_client_id"), MERGE_UPDATE_BEHAVIOR.get());

        assertTrue(bridgeRestarted.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

    @TestWithMqtt3Broker
    @WithKernel("config.yaml")
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_valid_mqttTopicMapping_updated_THEN_mapping_updated(Broker broker, ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, MqttException.class);

        TopicMapping topicMapping = testContext.getKernel().getContext().get(TopicMapping.class);
        assertThat(topicMapping.getMapping().size(), is(equalTo(0)));

        CountDownLatch bridgeRestarted = new CountDownLatch(1);
        testContext.getKernel().getContext().addGlobalStateChangeListener((GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(MQTTBridge.SERVICE_NAME) && newState.equals(State.NEW)) {
                bridgeRestarted.countDown();
            }
        });

        Topics mappingConfigTopics = testContext.getKernel().locate(MQTTBridge.SERVICE_NAME).getConfig()
                .lookupTopics(CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_MQTT_TOPIC_MAPPING);

        mappingConfigTopics.replaceAndWait(Utils.immutableMap("m1",
                Utils.immutableMap("topic", "mqtt/topic", "source", TopicMapping.TopicType.LocalMqtt.toString(),
                        "target", TopicMapping.TopicType.IotCore.toString()), "m2",
                Utils.immutableMap("topic", "mqtt/topic2", "source", TopicMapping.TopicType.LocalMqtt.toString(),
                        "target", TopicMapping.TopicType.Pubsub.toString()), "m3",
                Utils.immutableMap("topic", "mqtt/topic3", "source", TopicMapping.TopicType.LocalMqtt.toString(),
                        "target", TopicMapping.TopicType.IotCore.toString())));

        testContext.getKernel().getContext().waitForPublishQueueToClear();
        assertThat(topicMapping.getMapping().size(), is(equalTo(3)));
        assertFalse(bridgeRestarted.await(2, TimeUnit.SECONDS));
    }

    @TestWithMqtt3Broker
    @WithKernel("config_with_mapping.yaml")
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_valid_mapping_provided_in_config_THEN_mapping_populated(Broker broker, ExtensionContext context) {
        ignoreExceptionOfType(context, MqttException.class);

        TopicMapping topicMapping = testContext.getKernel().getContext().get(TopicMapping.class);

        assertThat(() -> topicMapping.getMapping().size(), eventuallyEval(is(5)));
        Map<String, TopicMapping.MappingEntry> expectedMapping = new HashMap<>();
        expectedMapping.put("mapping1",
                new TopicMapping.MappingEntry("topic/to/map/from/local/to/cloud", TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.IotCore));
        expectedMapping.put("mapping2",
                new TopicMapping.MappingEntry("topic/to/map/from/local/to/pubsub", TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.Pubsub));
        expectedMapping.put("mapping3",
                new TopicMapping.MappingEntry("topic/to/map/from/local/to/cloud/2", TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.IotCore));
        expectedMapping.put("mapping4",
                new TopicMapping.MappingEntry("topic/to/map/from/local/to/pubsub/2", TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.Pubsub, "a-prefix"));
        expectedMapping.put("mapping5",
                new TopicMapping.MappingEntry("topic/to/map/from/local/to/cloud/3", TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.IotCore, "a-prefix"));

        assertEquals(expectedMapping, topicMapping.getMapping());

        Map<String, Mqtt5RouteOptions> expectedRouteOptions = new HashMap<>();
        expectedRouteOptions.put("mapping1", Mqtt5RouteOptions.builder().noLocal(true).retainAsPublished(true).build());
        expectedRouteOptions.put("mappingNotInMqttTopicMapping",
                Mqtt5RouteOptions.builder().noLocal(true).retainAsPublished(false).build());

        Map<String, Mqtt5RouteOptions> actualRouteOptions =
                testContext.getKernel().getContext().get(BridgeConfigReference.class).get().getMqtt5RouteOptions();

        assertEquals(expectedRouteOptions, actualRouteOptions);
    }

    @TestWithMqtt3Broker
    @WithKernel("config.yaml")
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_empty_mqttTopicMapping_updated_THEN_mapping_not_updated(Broker broker, ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, MqttException.class);

        TopicMapping topicMapping = testContext.getKernel().getContext().get(TopicMapping.class);
        assertThat(topicMapping.getMapping().size(), is(equalTo(0)));

        testContext.getKernel().locate(MQTTBridge.SERVICE_NAME).getConfig()
                .lookupTopics(CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_MQTT_TOPIC_MAPPING)
                .replaceAndWait(Collections.emptyMap());
        // Block until subscriber has finished updating
        testContext.getKernel().getContext().waitForPublishQueueToClear();
        assertThat(topicMapping.getMapping().size(), is(equalTo(0)));
    }

    @TestWithMqtt3Broker
    @WithKernel("config_with_mapping.yaml")
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_mapping_updated_with_empty_THEN_mapping_removed(Broker broker, ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, MqttException.class);

        TopicMapping topicMapping = testContext.getKernel().getContext().get(TopicMapping.class);

        assertThat(() -> topicMapping.getMapping().size(), eventuallyEval(is(5)));
        testContext.getKernel().locate(MQTTBridge.SERVICE_NAME).getConfig()
                .lookupTopics(CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_MQTT_TOPIC_MAPPING)
                .replaceAndWait(Collections.emptyMap());
        // Block until subscriber has finished updating
        testContext.getKernel().getContext().waitForPublishQueueToClear();
        assertThat(topicMapping.getMapping().size(), is(equalTo(0)));
    }

    @TestWithMqtt3Broker
    @WithKernel("config.yaml")
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_invalid_mqttTopicMapping_updated_THEN_mapping_not_updated(Broker broker, ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, InvalidFormatException.class);

        TopicMapping topicMapping = testContext.getKernel().getContext().get(TopicMapping.class);
        assertThat(topicMapping.getMapping().size(), is(equalTo(0)));

        CountDownLatch bridgeErrored = new CountDownLatch(1);
        GlobalStateChangeListener listener = (GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(MQTTBridge.SERVICE_NAME) && service.getState().equals(State.ERRORED)) {
                bridgeErrored.countDown();
            }
        };
        testContext.getKernel().getContext().addGlobalStateChangeListener(listener);

        // Updating with invalid mapping (Providing type as Pubsub-Invalid)
        Topics mappingConfigTopics = testContext.getKernel().locate(MQTTBridge.SERVICE_NAME).getConfig()
                .lookupTopics(CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_MQTT_TOPIC_MAPPING);

        mappingConfigTopics.replaceAndWait(Utils.immutableMap("m1",
                Utils.immutableMap("topic", "mqtt/topic", "source", TopicMapping.TopicType.LocalMqtt.toString(),
                        "target", TopicMapping.TopicType.IotCore.toString()), "m2",
                Utils.immutableMap("topic", "mqtt/topic2", "source", TopicMapping.TopicType.LocalMqtt.toString(),
                        "target", "Pubsub-Invalid"), "m3",
                Utils.immutableMap("topic", "mqtt/topic3", "source", TopicMapping.TopicType.LocalMqtt.toString(),
                        "target", TopicMapping.TopicType.IotCore.toString())));

        // Block until subscriber has finished updating
        testContext.getKernel().getContext().waitForPublishQueueToClear();
        assertThat(topicMapping.getMapping().size(), is(equalTo(0)));

        assertTrue(bridgeErrored.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    @TestWithMqtt5Broker
    @WithKernel("config.yaml")
    void GIVEN_bridge_WHEN_mqtt_version_toggled_THEN_clients_switched(Broker broker, ExtensionContext context) throws Exception {
        MQTTClient v3Client = testContext.getLocalV3Client();
        assertTrue(v3Client.getMqttClientInternal().isConnected());

        // switch mqtt version from mqtt3 to mqtt5
        testContext.getKernel().locate(MQTTBridge.SERVICE_NAME)
                .getConfig()
                .lookupTopics(CONFIGURATION_CONFIG_KEY)
                .lookup(BridgeConfig.KEY_MQTT, BridgeConfig.KEY_VERSION)
                .withValue("mqtt5");

        // wait for clients to switch
        assertThat("mqtt5 client active", () -> {
            try {
                testContext.getLocalV5Client();
                return true;
            } catch (Exception e) {
                return false;
            }
        }, eventuallyEval(is(true)));

        assertFalse(v3Client.getMqttClientInternal().isConnected());
        try {
            v3Client.getMqttClientInternal().connect();
            fail("v3 client expected to be closed");
        } catch (MqttException e) {
            assertEquals(MqttException.REASON_CODE_CLIENT_CLOSED, e.getReasonCode());
        }

        assertThat("mqtt5 client connected", () -> testContext.getLocalV5Client().getClient().getIsConnected(), eventuallyEval(is(true)));
    }
}
