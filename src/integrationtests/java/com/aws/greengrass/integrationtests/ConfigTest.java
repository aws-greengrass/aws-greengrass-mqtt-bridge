/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.config.Topics;
import com.aws.greengrass.config.UpdateBehaviorTree;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.integrationtests.extensions.BridgeIntegrationTest;
import com.aws.greengrass.integrationtests.extensions.BridgeIntegrationTestContext;
import com.aws.greengrass.integrationtests.extensions.Broker;
import com.aws.greengrass.integrationtests.extensions.TestWithMqtt3Broker;
import com.aws.greengrass.integrationtests.extensions.WithKernel;
import com.aws.greengrass.lifecyclemanager.GlobalStateChangeListener;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import com.aws.greengrass.mqtt.bridge.BridgeConfig;
import com.aws.greengrass.mqtt.bridge.MQTTBridge;
import com.aws.greengrass.mqtt.bridge.TopicMapping;
import com.aws.greengrass.mqtt.bridge.model.Mqtt5RouteOptions;
import com.aws.greengrass.util.Utils;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.github.grantwest.eventually.EventuallyLambdaMatcher;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@BridgeIntegrationTest
public class ConfigTest {
    private static final long TEST_TIME_OUT_SEC = 30L;
    private static final Supplier<UpdateBehaviorTree> MERGE_UPDATE_BEHAVIOR =
            () -> new UpdateBehaviorTree(UpdateBehaviorTree.UpdateBehavior.MERGE, System.currentTimeMillis());

    BridgeIntegrationTestContext testContext;

    @TestWithMqtt3Broker
    @WithKernel("config.yaml")
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
        config.updateFromMap(Utils.immutableMap(BridgeConfig.KEY_BROKER_URI, "tcp://newbroker:1234"), MERGE_UPDATE_BEHAVIOR.get());

        assertTrue(bridgeRestarted.await(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
        assertEquals(1, numRestarts.get());
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
            config.updateFromMap(Utils.immutableMap(BridgeConfig.KEY_BROKER_URI, String.format("tcp://brokeruri:%d", i)), MERGE_UPDATE_BEHAVIOR.get());
            assertTrue(bridgeRestarted.tryAcquire(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
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

        assertTrue(bridgeRestarted.await(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
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

        assertThat(() -> topicMapping.getMapping().size(), EventuallyLambdaMatcher.eventuallyEval(is(5)));
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
        expectedRouteOptions.put("mapping1", Mqtt5RouteOptions.builder().noLocal(true).retainAsPublished(false).build());
        expectedRouteOptions.put("mapping2", Mqtt5RouteOptions.builder().noLocal(false).retainAsPublished(true).build());
        expectedRouteOptions.put("mappingNotInMqttTopicMapping", Mqtt5RouteOptions.builder().noLocal(true).retainAsPublished(true).build());

        Map<String, Mqtt5RouteOptions> actualRouteOptions =
                testContext.getKernel().getContext().get(MQTTBridge.class).getBridgeConfig().getMqtt5RouteOptions();

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

        assertThat(() -> topicMapping.getMapping().size(), EventuallyLambdaMatcher.eventuallyEval(is(5)));
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

        assertTrue(bridgeErrored.await(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
    }
}
