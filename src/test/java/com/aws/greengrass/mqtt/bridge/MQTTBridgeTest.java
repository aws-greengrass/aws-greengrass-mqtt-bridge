/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge;

import com.aws.greengrass.builtin.services.pubsub.PubSubIPCEventStreamAgent;
import com.aws.greengrass.clientdevices.auth.CertificateManager;
import com.aws.greengrass.componentmanager.KernelConfigResolver;
import com.aws.greengrass.config.Topic;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.config.UpdateBehaviorTree;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.clientdevices.auth.ClientDevicesAuthService;
import com.aws.greengrass.lifecyclemanager.GlobalStateChangeListener;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.mqtt.bridge.auth.MQTTClientKeyStore;
import com.aws.greengrass.mqtt.bridge.clients.IoTCoreClient;
import com.aws.greengrass.mqtt.bridge.clients.MQTTClient;
import com.aws.greengrass.mqtt.bridge.clients.PubSubClient;
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import com.aws.greengrass.util.Utils;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.github.grantwest.eventually.EventuallyLambdaMatcher;
import io.moquette.BrokerConstants;
import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
@Disabled
public class MQTTBridgeTest extends GGServiceTestUtil {
    private static final long TEST_TIME_OUT_SEC = 30L;

    private static final Supplier<UpdateBehaviorTree> MERGE_UPDATE_BEHAVIOR =
            () -> new UpdateBehaviorTree(UpdateBehaviorTree.UpdateBehavior.MERGE, System.currentTimeMillis());

    private Kernel kernel;
    private GlobalStateChangeListener listener;
    private Server broker;
    private ScheduledExecutorService ses;

    @TempDir
    Path rootDir;

    @Mock
    CertificateManager mockCertificateManager;

    @BeforeEach
    void setup() throws IOException {
        // Set this property for kernel to scan its own classpath to find plugins
        System.setProperty("aws.greengrass.scanSelfClasspath", "true");

        kernel = new Kernel();
        kernel.getContext().put(CertificateManager.class, mockCertificateManager);
        IConfig defaultConfig = new MemoryConfig(new Properties());
        defaultConfig.setProperty(BrokerConstants.PORT_PROPERTY_NAME, "8883");
        broker = new Server();
        broker.startServer(defaultConfig);
        ses = new ScheduledThreadPoolExecutor(1);
    }

    @AfterEach
    void cleanup() {
        kernel.shutdown();
        broker.stopServer();
        ses.shutdownNow();
    }

    private void startKernelWithConfig(String configFileName) throws InterruptedException {
        CountDownLatch bridgeRunning = new CountDownLatch(1);
        kernel.parseArgs("-r", rootDir.toAbsolutePath().toString(), "-i",
                getClass().getResource(configFileName).toString());
        listener = (GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(MQTTBridge.SERVICE_NAME) && service.getState().equals(State.RUNNING)) {
                bridgeRunning.countDown();
            }
        };
        kernel.getContext().addGlobalStateChangeListener(listener);
        kernel.launch();

        Assertions.assertTrue(bridgeRunning.await(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_multiple_config_changes_consecutively_THEN_bridge_reinstalls_once(ExtensionContext context)
            throws Exception {
        ignoreExceptionOfType(context, InterruptedException.class);
        startKernelWithConfig("config.yaml");

        CountDownLatch bridgeRestarted = new CountDownLatch(1);
        AtomicInteger numRestarts = new AtomicInteger();

        kernel.getContext().addGlobalStateChangeListener((GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(MQTTBridge.SERVICE_NAME) && newState.equals(State.NEW)) {
                numRestarts.incrementAndGet();
                bridgeRestarted.countDown();
            }
        });

        Topics config = kernel.locate(MQTTBridge.SERVICE_NAME).getConfig()
                .lookupTopics(KernelConfigResolver.CONFIGURATION_CONFIG_KEY);

        config.updateFromMap(Utils.immutableMap(BridgeConfig.KEY_CLIENT_ID, "new_client_id"), MERGE_UPDATE_BEHAVIOR.get());
        config.updateFromMap(Utils.immutableMap(BridgeConfig.KEY_BROKER_URI, "tcp://newbroker:1234"), MERGE_UPDATE_BEHAVIOR.get());

        Assertions.assertTrue(bridgeRestarted.await(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
        assertEquals(1, numRestarts.get());
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_multiple_serialized_config_changes_occur_THEN_bridge_reinstalls_multiple_times(ExtensionContext context)
            throws Exception {
        ignoreExceptionOfType(context, InterruptedException.class);
        startKernelWithConfig("config.yaml");

        Semaphore bridgeRestarted = new Semaphore(1);
        bridgeRestarted.acquire();

        kernel.getContext().addGlobalStateChangeListener((GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(MQTTBridge.SERVICE_NAME) && newState.equals(State.RUNNING)) {
                bridgeRestarted.release();
            }
        });

        Topics config = kernel.locate(MQTTBridge.SERVICE_NAME).getConfig()
                .lookupTopics(KernelConfigResolver.CONFIGURATION_CONFIG_KEY);

        int numRestarts = 5;
        for (int i = 0; i < numRestarts; i++) {
            // change the configuration and wait for bridge to restart
            config.updateFromMap(Utils.immutableMap(BridgeConfig.KEY_BROKER_URI, String.format("tcp://brokeruri:%d", i)), MERGE_UPDATE_BEHAVIOR.get());
            Assertions.assertTrue(bridgeRestarted.tryAcquire(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
        }
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_clientId_config_changes_THEN_bridge_reinstalls() throws Exception {
        startKernelWithConfig("config.yaml");

        CountDownLatch bridgeRestarted = new CountDownLatch(1);
        kernel.getContext().addGlobalStateChangeListener((GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(MQTTBridge.SERVICE_NAME) && newState.equals(State.NEW)) {
                bridgeRestarted.countDown();
            }
        });

        Topics config = kernel.locate(MQTTBridge.SERVICE_NAME).getConfig()
                .lookupTopics(KernelConfigResolver.CONFIGURATION_CONFIG_KEY);
        config.updateFromMap(Utils.immutableMap(BridgeConfig.KEY_CLIENT_ID, "new_client_id"), MERGE_UPDATE_BEHAVIOR.get());

        Assertions.assertTrue(bridgeRestarted.await(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_start_kernel_THEN_bridge_starts_successfully() throws Exception {
        startKernelWithConfig("config.yaml");
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_valid_mqttTopicMapping_updated_THEN_mapping_updated(ExtensionContext context)
            throws Exception {
        ignoreExceptionOfType(context, MqttException.class);
        startKernelWithConfig("config.yaml");
        TopicMapping topicMapping = ((MQTTBridge) kernel.locate(MQTTBridge.SERVICE_NAME)).getTopicMapping();
        assertThat(topicMapping.getMapping().size(), is(equalTo(0)));

        CountDownLatch bridgeRestarted = new CountDownLatch(1);
        kernel.getContext().addGlobalStateChangeListener((GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(MQTTBridge.SERVICE_NAME) && newState.equals(State.NEW)) {
                bridgeRestarted.countDown();
            }
        });

        Topics mappingConfigTopics = kernel.locate(MQTTBridge.SERVICE_NAME).getConfig()
                .lookupTopics(KernelConfigResolver.CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_MQTT_TOPIC_MAPPING);

        mappingConfigTopics.replaceAndWait(Utils.immutableMap("m1",
                Utils.immutableMap("topic", "mqtt/topic", "source", TopicMapping.TopicType.LocalMqtt.toString(),
                        "target", TopicMapping.TopicType.IotCore.toString()), "m2",
                Utils.immutableMap("topic", "mqtt/topic2", "source", TopicMapping.TopicType.LocalMqtt.toString(),
                        "target", TopicMapping.TopicType.Pubsub.toString()), "m3",
                Utils.immutableMap("topic", "mqtt/topic3", "source", TopicMapping.TopicType.LocalMqtt.toString(),
                        "target", TopicMapping.TopicType.IotCore.toString())));

        kernel.getContext().runOnPublishQueueAndWait(() -> {
        });
        assertThat(topicMapping.getMapping().size(), is(equalTo(3)));
        assertFalse(bridgeRestarted.await(2, TimeUnit.SECONDS));
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_valid_mapping_provided_in_config_THEN_mapping_populated(ExtensionContext context)
            throws Exception {
        ignoreExceptionOfType(context, MqttException.class);
        startKernelWithConfig("config_with_mapping.yaml");
        TopicMapping topicMapping = ((MQTTBridge) kernel.locate(MQTTBridge.SERVICE_NAME)).getTopicMapping();

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
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_empty_mqttTopicMapping_updated_THEN_mapping_not_updated(ExtensionContext context)
            throws Exception {
        ignoreExceptionOfType(context, MqttException.class);
        startKernelWithConfig("config.yaml");
        TopicMapping topicMapping = ((MQTTBridge) kernel.locate(MQTTBridge.SERVICE_NAME)).getTopicMapping();
        assertThat(topicMapping.getMapping().size(), is(equalTo(0)));

        kernel.locate(MQTTBridge.SERVICE_NAME).getConfig()
                .lookupTopics(KernelConfigResolver.CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_MQTT_TOPIC_MAPPING)
                .replaceAndWait(Collections.EMPTY_MAP);
        // Block until subscriber has finished updating
        kernel.getContext().runOnPublishQueueAndWait(() -> {
        });
        assertThat(topicMapping.getMapping().size(), is(equalTo(0)));
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_mapping_updated_with_empty_THEN_mapping_removed(ExtensionContext context)
            throws Exception {
        ignoreExceptionOfType(context, MqttException.class);
        startKernelWithConfig("config_with_mapping.yaml");
        TopicMapping topicMapping = ((MQTTBridge) kernel.locate(MQTTBridge.SERVICE_NAME)).getTopicMapping();

        assertThat(() -> topicMapping.getMapping().size(), EventuallyLambdaMatcher.eventuallyEval(is(5)));
        kernel.locate(MQTTBridge.SERVICE_NAME).getConfig()
                .lookupTopics(KernelConfigResolver.CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_MQTT_TOPIC_MAPPING)
                .replaceAndWait(Collections.EMPTY_MAP);
        // Block until subscriber has finished updating
        kernel.getContext().runOnPublishQueueAndWait(() -> {
        });
        assertThat(topicMapping.getMapping().size(), is(equalTo(0)));
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_invalid_mqttTopicMapping_updated_THEN_mapping_not_updated(ExtensionContext context)
            throws Exception {
        ignoreExceptionOfType(context, InvalidFormatException.class);
        startKernelWithConfig("config.yaml");
        TopicMapping topicMapping = ((MQTTBridge) kernel.locate(MQTTBridge.SERVICE_NAME)).getTopicMapping();
        assertThat(topicMapping.getMapping().size(), is(equalTo(0)));

        kernel.getContext().removeGlobalStateChangeListener(listener);
        CountDownLatch bridgeErrored = new CountDownLatch(1);

        GlobalStateChangeListener listener = (GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(MQTTBridge.SERVICE_NAME) && service.getState().equals(State.ERRORED)) {
                bridgeErrored.countDown();
            }
        };

        kernel.getContext().addGlobalStateChangeListener(listener);

        // Updating with invalid mapping (Providing type as Pubsub-Invalid)
        Topics mappingConfigTopics = kernel.locate(MQTTBridge.SERVICE_NAME).getConfig()
                .lookupTopics(KernelConfigResolver.CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_MQTT_TOPIC_MAPPING);

        mappingConfigTopics.replaceAndWait(Utils.immutableMap("m1",
                Utils.immutableMap("topic", "mqtt/topic", "source", TopicMapping.TopicType.LocalMqtt.toString(),
                        "target", TopicMapping.TopicType.IotCore.toString()), "m2",
                Utils.immutableMap("topic", "mqtt/topic2", "source", TopicMapping.TopicType.LocalMqtt.toString(),
                        "target", "Pubsub-Invalid"), "m3",
                Utils.immutableMap("topic", "mqtt/topic3", "source", TopicMapping.TopicType.LocalMqtt.toString(),
                        "target", TopicMapping.TopicType.IotCore.toString())));

        // Block until subscriber has finished updating
        kernel.getContext().runOnPublishQueueAndWait(() -> {
        });
        assertThat(topicMapping.getMapping().size(), is(equalTo(0)));

        Assertions.assertTrue(bridgeErrored.await(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_CAs_updated_THEN_KeyStore_updated(ExtensionContext extensionContext) throws Exception {
        ignoreExceptionOfType(extensionContext, InterruptedException.class);
        serviceFullName = MQTTBridge.SERVICE_NAME;
        initializeMockedConfig();
        TopicMapping mockTopicMapping = mock(TopicMapping.class);
        MessageBridge mockMessageBridge = mock(MessageBridge.class);
        PubSubIPCEventStreamAgent mockPubSubIPCAgent = mock(PubSubIPCEventStreamAgent.class);
        Kernel mockKernel = mock(Kernel.class);
        MQTTClientKeyStore mockMqttClientKeyStore = mock(MQTTClientKeyStore.class);
        MQTTBridge mqttBridge;

        Topics config = Topics.of(context, KernelConfigResolver.CONFIGURATION_CONFIG_KEY, null);
        config.lookup(KernelConfigResolver.CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_BROKER_URI)
                .dflt("tcp://localhost:8883");
        config.lookup(KernelConfigResolver.CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_CLIENT_ID)
                .dflt(MQTTBridge.SERVICE_NAME);

        try (MqttClient mockIotMqttClient = mock(MqttClient.class)) {
            mqttBridge =
                    new MQTTBridge(config, mockTopicMapping, mockMessageBridge, mockPubSubIPCAgent, mockIotMqttClient,
                            mockKernel, mockMqttClientKeyStore, ses);
        }

        ClientDevicesAuthService mockClientAuthService = mock(ClientDevicesAuthService.class);
        when(mockKernel.locate(ClientDevicesAuthService.CLIENT_DEVICES_AUTH_SERVICE_NAME))
                .thenReturn(mockClientAuthService);
        Topics mockClientAuthConfig = mock(Topics.class);
        when(mockClientAuthService.getConfig()).thenReturn(mockClientAuthConfig);

        Topic caTopic = Topic.of(context, "authorities", Arrays.asList("CA1", "CA2"));
        when(mockClientAuthConfig
                .lookup(MQTTBridge.RUNTIME_STORE_NAMESPACE_TOPIC, ClientDevicesAuthService.CERTIFICATES_KEY,
                        ClientDevicesAuthService.AUTHORITIES_TOPIC)).thenReturn(caTopic);
        mqttBridge.install();
        mqttBridge.startup();
        mqttBridge.shutdown();
        ArgumentCaptor<List<String>> caListCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockMqttClientKeyStore).updateCA(caListCaptor.capture());
        assertThat(caListCaptor.getValue(), is(Arrays.asList("CA1", "CA2")));

        caTopic = Topic.of(context, "authorities", Collections.emptyList());
        when(mockClientAuthConfig
                .lookup(MQTTBridge.RUNTIME_STORE_NAMESPACE_TOPIC, ClientDevicesAuthService.CERTIFICATES_KEY,
                        ClientDevicesAuthService.AUTHORITIES_TOPIC)).thenReturn(caTopic);
        reset(mockMqttClientKeyStore);
        mqttBridge.install();
        mqttBridge.startup();
        mqttBridge.shutdown();
        verify(mockMqttClientKeyStore, never()).updateCA(caListCaptor.capture());
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_Startup_THEN_Mqtt_subscriptions_updated_for_all_clients(ExtensionContext extensionContext) throws Exception {
        ignoreExceptionOfType(extensionContext, InterruptedException.class);
        serviceFullName = MQTTBridge.SERVICE_NAME;
        initializeMockedConfig();
        TopicMapping mockTopicMapping = mock(TopicMapping.class);
        MessageBridge mockMessageBridge = mock(MessageBridge.class);
        Kernel mockKernel = mock(Kernel.class);
        MQTTClientKeyStore mockMqttClientKeyStore = mock(MQTTClientKeyStore.class);

        Topics config = Topics.of(context, KernelConfigResolver.CONFIGURATION_CONFIG_KEY, null);
        config.lookup(KernelConfigResolver.CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_BROKER_URI)
                .dflt("tcp://localhost:8883");
        config.lookup(KernelConfigResolver.CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_CLIENT_ID)
                .dflt(MQTTBridge.SERVICE_NAME);

        MQTTBridge mqttBridge =
                new MQTTBridge(config, mockTopicMapping, mockMessageBridge, mock(PubSubIPCEventStreamAgent.class),
                        mock(MqttClient.class), mockKernel, mockMqttClientKeyStore, ses);

        ClientDevicesAuthService mockClientAuthService = mock(ClientDevicesAuthService.class);
        when(mockKernel.locate(ClientDevicesAuthService.CLIENT_DEVICES_AUTH_SERVICE_NAME))
                .thenReturn(mockClientAuthService);
        Topics mockClientAuthConfig = mock(Topics.class);
        when(mockClientAuthService.getConfig()).thenReturn(mockClientAuthConfig);

        Topic caTopic = Topic.of(context, "authorities", Arrays.asList("CA1", "CA2"));
        when(mockClientAuthConfig
                .lookup(MQTTBridge.RUNTIME_STORE_NAMESPACE_TOPIC, ClientDevicesAuthService.CERTIFICATES_KEY,
                        ClientDevicesAuthService.AUTHORITIES_TOPIC)).thenReturn(caTopic);

        mqttBridge.install();
        mqttBridge.startup();
        mqttBridge.shutdown();

        verify(mockMessageBridge).addOrReplaceMessageClientAndUpdateSubscriptions(
                eq(TopicMapping.TopicType.LocalMqtt), any(MQTTClient.class));
        verify(mockMessageBridge).addOrReplaceMessageClientAndUpdateSubscriptions(
                eq(TopicMapping.TopicType.Pubsub), any(PubSubClient.class));
        verify(mockMessageBridge).addOrReplaceMessageClientAndUpdateSubscriptions(
                eq(TopicMapping.TopicType.IotCore), any(IoTCoreClient.class));
    }
}
