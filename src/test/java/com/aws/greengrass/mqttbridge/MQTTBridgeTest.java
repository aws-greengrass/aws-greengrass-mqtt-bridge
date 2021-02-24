/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge;

import com.aws.greengrass.certificatemanager.CertificateManager;
import com.aws.greengrass.certificatemanager.DCMService;
import com.aws.greengrass.componentmanager.KernelConfigResolver;
import com.aws.greengrass.config.Topic;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.lifecyclemanager.GlobalStateChangeListener;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.mqttbridge.auth.MQTTClientKeyStore;
import com.aws.greengrass.mqttbridge.clients.IoTCoreClient;
import com.aws.greengrass.mqttbridge.clients.MQTTClient;
import com.aws.greengrass.mqttbridge.clients.PubSubClient;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import com.github.grantwest.eventually.EventuallyLambdaMatcher;
import io.moquette.BrokerConstants;
import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class MQTTBridgeTest extends GGServiceTestUtil {
    private static final long TEST_TIME_OUT_SEC = 30L;

    private Kernel kernel;
    private GlobalStateChangeListener listener;
    private Server broker;

    @TempDir
    Path rootDir;

    @Mock
    CertificateManager mockCertificateManager;

    @BeforeEach
    void setup() throws IOException {
        kernel = new Kernel();
        kernel.getContext().put(CertificateManager.class, mockCertificateManager);
        IConfig defaultConfig = new MemoryConfig(new Properties());
        defaultConfig.setProperty(BrokerConstants.PORT_PROPERTY_NAME, "8883");
        broker = new Server();
        broker.startServer(defaultConfig);
    }

    @AfterEach
    void cleanup() {
        kernel.shutdown();
        broker.stopServer();
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
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_start_kernel_THEN_bridge_starts_successfully() throws Exception {
        startKernelWithConfig("config.yaml");
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_valid_mqttTopicMapping_updated_THEN_mapping_updated() throws Exception {
        startKernelWithConfig("config.yaml");
        TopicMapping topicMapping = ((MQTTBridge) kernel.locate(MQTTBridge.SERVICE_NAME)).getTopicMapping();
        assertThat(topicMapping.getMapping().size(), is(equalTo(0)));

        kernel.locate(MQTTBridge.SERVICE_NAME).getConfig()
                .find(KernelConfigResolver.CONFIGURATION_CONFIG_KEY, MQTTBridge.MQTT_TOPIC_MAPPING).withValue("[\n"
                + "  {\"SourceTopic\": \"mqtt/topic\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/cloud/topic\", \"DestTopicType\": \"IotCore\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic2\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/pubsub/topic\", \"DestTopicType\": \"Pubsub\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic3\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/cloud/topic2\", \"DestTopicType\": \"IotCore\"}\n"
                + "]");
        // Block until subscriber has finished updating
        kernel.getContext().runOnPublishQueueAndWait(() -> {
        });
        assertThat(topicMapping.getMapping().size(), is(equalTo(3)));
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_valid_mapping_provided_in_config_THEN_mapping_populated()
            throws Exception {
        startKernelWithConfig("config_with_mapping.yaml");
        TopicMapping topicMapping = ((MQTTBridge) kernel.locate(MQTTBridge.SERVICE_NAME)).getTopicMapping();

        assertThat(() -> topicMapping.getMapping().size(), EventuallyLambdaMatcher.eventuallyEval(is(3)));
        List<TopicMapping.MappingEntry> expectedMapping = new ArrayList<>();
        expectedMapping.add(new TopicMapping.MappingEntry("topic/to/iotCore", TopicMapping.TopicType.LocalMqtt,
                "/test/cloud/topic", TopicMapping.TopicType.IotCore));
        expectedMapping.add(new TopicMapping.MappingEntry("topic/to/pubsub", TopicMapping.TopicType.LocalMqtt,
                "/test/pubsub/topic", TopicMapping.TopicType.Pubsub));
        expectedMapping.add(new TopicMapping.MappingEntry("topic/to/iotCore/2", TopicMapping.TopicType.LocalMqtt,
                "/test/cloud/topic2", TopicMapping.TopicType.IotCore));

        assertArrayEquals(expectedMapping.toArray(), topicMapping.getMapping().toArray());
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_empty_mqttTopicMapping_updated_THEN_mapping_not_updated()
            throws Exception {
        startKernelWithConfig("config.yaml");
        TopicMapping topicMapping = ((MQTTBridge) kernel.locate(MQTTBridge.SERVICE_NAME)).getTopicMapping();
        assertThat(topicMapping.getMapping().size(), is(equalTo(0)));

        kernel.locate(MQTTBridge.SERVICE_NAME).getConfig()
                .find(KernelConfigResolver.CONFIGURATION_CONFIG_KEY, MQTTBridge.MQTT_TOPIC_MAPPING).withValue("");
        // Block until subscriber has finished updating
        kernel.getContext().runOnPublishQueueAndWait(() -> {
        });
        assertThat(topicMapping.getMapping().size(), is(equalTo(0)));
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_invalid_mqttTopicMapping_updated_THEN_mapping_not_updated()
            throws Exception {
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
        kernel.locate(MQTTBridge.SERVICE_NAME).getConfig()
                .find(KernelConfigResolver.CONFIGURATION_CONFIG_KEY, MQTTBridge.MQTT_TOPIC_MAPPING).withValue("[\n"
                + "  {\"SourceTopic\": \"mqtt/topic\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/cloud/topic\", \"DestTopicType\": \"IotCore\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic2\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": "
                + "\"/test/pubsub/topic\", \"DestTopicType\": \"Pubsub-Invalid\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic3\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/cloud/topic2\", \"DestTopicType\": \"IotCore\"}\n"
                + "]");
        // Block until subscriber has finished updating
        kernel.getContext().runOnPublishQueueAndWait(() -> {
        });
        assertThat(topicMapping.getMapping().size(), is(equalTo(0)));

        Assertions.assertTrue(bridgeErrored.await(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_CAs_updated_THEN_KeyStore_updated() throws Exception {
        serviceFullName = MQTTBridge.SERVICE_NAME;
        initializeMockedConfig();
        TopicMapping mockTopicMapping = mock(TopicMapping.class);
        MessageBridge mockMessageBridge = mock(MessageBridge.class);
        PubSubClient mockPubSubClient = mock(PubSubClient.class);
        IoTCoreClient mockIoTCoreClient = mock(IoTCoreClient.class);
        Kernel mockKernel = mock(Kernel.class);
        MQTTClientKeyStore mockMqttClientKeyStore = mock(MQTTClientKeyStore.class);
        MQTTBridge mqttBridge = new MQTTBridge(config, mockTopicMapping, mockMessageBridge, mockPubSubClient,
                mockIoTCoreClient, mockKernel, mockMqttClientKeyStore);

        when(config.findOrDefault(any(), eq(KernelConfigResolver.CONFIGURATION_CONFIG_KEY),
                eq(MQTTClient.BROKER_URI_KEY))).thenReturn("tcp://localhost:8883");
        when(config.findOrDefault(any(), eq(KernelConfigResolver.CONFIGURATION_CONFIG_KEY),
                eq(MQTTClient.CLIENT_ID_KEY))).thenReturn(MQTTBridge.SERVICE_NAME);
        when(config.findOrDefault(any(), eq(KernelConfigResolver.CONFIGURATION_CONFIG_KEY),
                eq(MQTTClient.MAX_RETRIES_KEY))).thenReturn(10);

        DCMService mockDCMService = mock(DCMService.class);
        when(mockKernel.locate(DCMService.DCM_SERVICE_NAME)).thenReturn(mockDCMService);
        Topics mockDCMConfig = mock(Topics.class);
        when(mockDCMService.getConfig()).thenReturn(mockDCMConfig);

        Topic caTopic = Topic.of(context, "authorities", Arrays.asList("CA1", "CA2"));
        when(mockDCMConfig.lookup(MQTTBridge.RUNTIME_STORE_NAMESPACE_TOPIC, DCMService.CERTIFICATES_KEY,
                DCMService.AUTHORITIES_TOPIC)).thenReturn(caTopic);
        mqttBridge.startup();
        mqttBridge.shutdown();
        ArgumentCaptor<List<String>> caListCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockMqttClientKeyStore).updateCA(caListCaptor.capture());
        assertThat(caListCaptor.getValue(), is(Arrays.asList("CA1", "CA2")));

        caTopic = Topic.of(context, "authorities", Collections.emptyList());
        when(mockDCMConfig.lookup(MQTTBridge.RUNTIME_STORE_NAMESPACE_TOPIC, DCMService.CERTIFICATES_KEY,
                DCMService.AUTHORITIES_TOPIC)).thenReturn(caTopic);
        reset(mockMqttClientKeyStore);
        mqttBridge.startup();
        mqttBridge.shutdown();
        verify(mockMqttClientKeyStore, never()).updateCA(caListCaptor.capture());
    }
}
