package com.aws.iot.evergreen.mqtt.bridge;

import com.aws.iot.evergreen.config.Topic;
import com.aws.iot.evergreen.config.Topics;
import com.aws.iot.evergreen.dcm.certificate.CertificateManager;
import com.aws.iot.evergreen.dcm.DCMService;
import com.aws.iot.evergreen.dependency.State;
import com.aws.iot.evergreen.kernel.EvergreenService;
import com.aws.iot.evergreen.kernel.GlobalStateChangeListener;
import com.aws.iot.evergreen.kernel.Kernel;

import com.aws.iot.evergreen.mqtt.bridge.auth.MQTTClientKeyStore;
import com.aws.iot.evergreen.mqtt.bridge.clients.MQTTClient;
import com.aws.iot.evergreen.packagemanager.KernelConfigResolver;
import com.aws.iot.evergreen.testcommons.testutilities.EGExtension;
import com.aws.iot.evergreen.testcommons.testutilities.EGServiceTestUtil;
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

@ExtendWith({MockitoExtension.class, EGExtension.class})
public class MQTTBridgeTest extends EGServiceTestUtil {
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
        listener = (EvergreenService service, State was, State newState) -> {
            if (service.getName().equals(MQTTBridge.SERVICE_NAME) && service.getState().equals(State.RUNNING)) {
                bridgeRunning.countDown();
            }
        };
        kernel.getContext().addGlobalStateChangeListener(listener);
        kernel.launch();

        Assertions.assertTrue(bridgeRunning.await(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    void GIVEN_Evergreen_with_mqtt_bridge_WHEN_start_kernel_THEN_bridge_starts_successfully() throws Exception {
        startKernelWithConfig("config.yaml");
    }

    @Test
    void GIVEN_Evergreen_with_mqtt_bridge_WHEN_valid_mqttTopicMapping_updated_THEN_mapping_updated() throws Exception {
        startKernelWithConfig("config.yaml");
        TopicMapping topicMapping = ((MQTTBridge) kernel.locate(MQTTBridge.SERVICE_NAME)).getTopicMapping();
        assertThat(topicMapping.getMapping().size(), is(equalTo(0)));

        kernel.locate(MQTTBridge.SERVICE_NAME).getConfig()
                .find(KernelConfigResolver.PARAMETERS_CONFIG_KEY, MQTTBridge.MQTT_TOPIC_MAPPING).withValue("[\n"
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
    void GIVEN_Evergreen_with_mqtt_bridge_WHEN_valid_mapping_provided_in_config_THEN_mapping_populated()
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
    void GIVEN_Evergreen_with_mqtt_bridge_WHEN_empty_mqttTopicMapping_updated_THEN_mapping_not_updated()
            throws Exception {
        startKernelWithConfig("config.yaml");
        TopicMapping topicMapping = ((MQTTBridge) kernel.locate(MQTTBridge.SERVICE_NAME)).getTopicMapping();
        assertThat(topicMapping.getMapping().size(), is(equalTo(0)));

        kernel.locate(MQTTBridge.SERVICE_NAME).getConfig()
                .find(KernelConfigResolver.PARAMETERS_CONFIG_KEY, MQTTBridge.MQTT_TOPIC_MAPPING).withValue("");
        // Block until subscriber has finished updating
        kernel.getContext().runOnPublishQueueAndWait(() -> {
        });
        assertThat(topicMapping.getMapping().size(), is(equalTo(0)));
    }

    @Test
    void GIVEN_Evergreen_with_mqtt_bridge_WHEN_invalid_mqttTopicMapping_updated_THEN_mapping_not_updated()
            throws Exception {
        startKernelWithConfig("config.yaml");
        TopicMapping topicMapping = ((MQTTBridge) kernel.locate(MQTTBridge.SERVICE_NAME)).getTopicMapping();
        assertThat(topicMapping.getMapping().size(), is(equalTo(0)));

        kernel.getContext().removeGlobalStateChangeListener(listener);
        CountDownLatch bridgeErrored = new CountDownLatch(1);

        GlobalStateChangeListener listener = (EvergreenService service, State was, State newState) -> {
            if (service.getName().equals(MQTTBridge.SERVICE_NAME) && service.getState().equals(State.ERRORED)) {
                bridgeErrored.countDown();
            }
        };

        kernel.getContext().addGlobalStateChangeListener(listener);

        // Updating with invalid mapping (Providing type as Pubsub-Invalid)
        kernel.locate(MQTTBridge.SERVICE_NAME).getConfig()
                .find(KernelConfigResolver.PARAMETERS_CONFIG_KEY, MQTTBridge.MQTT_TOPIC_MAPPING).withValue("[\n"
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
    void GIVEN_Evergreen_with_mqtt_bridge_WHEN_CAs_updated_THEN_KeyStore_updated() throws Exception {
        serviceFullName = MQTTBridge.SERVICE_NAME;
        initializeMockedConfig();
        TopicMapping mockTopicMapping = mock(TopicMapping.class);
        MessageBridge mockMessageBridge = mock(MessageBridge.class);
        Kernel mockKernel = mock(Kernel.class);
        MQTTClientKeyStore mockMqttClientKeyStore = mock(MQTTClientKeyStore.class);

        Topic mappingTopic = mock(Topic.class);
        when(mappingTopic.dflt(any())).thenReturn(mappingTopic);
        when(mappingTopic.subscribe(any())).thenAnswer((a) -> null);
        when(config.lookup(KernelConfigResolver.PARAMETERS_CONFIG_KEY, MQTTBridge.MQTT_TOPIC_MAPPING))
                .thenReturn(mappingTopic);

        DCMService mockDCMService = mock(DCMService.class);
        when(mockKernel.locate(DCMService.DCM_SERVICE_NAME)).thenReturn(mockDCMService);
        Topics mockDCMConfig = mock(Topics.class);
        when(mockDCMService.getConfig()).thenReturn(mockDCMConfig);

        when(config.findOrDefault(any(), eq(KernelConfigResolver.PARAMETERS_CONFIG_KEY),
                eq(MQTTClient.BROKER_URI_KEY))).thenReturn("tcp://localhost:8883");
        when(config.findOrDefault(any(), eq(KernelConfigResolver.PARAMETERS_CONFIG_KEY),
                eq(MQTTClient.CLIENT_ID_KEY))).thenReturn(MQTTBridge.SERVICE_NAME);

        Topic caTopic = Topic.of(context, "authorities", Arrays.asList("CA1", "CA2"));
        when(mockDCMConfig.lookup(MQTTBridge.RUNTIME_CONFIG_KEY, MQTTBridge.CERTIFICATES_TOPIC, MQTTBridge.AUTHORITIES))
                .thenReturn(caTopic);
        new MQTTBridge(config, mockTopicMapping, mockMessageBridge, mockKernel, mockMqttClientKeyStore);
        ArgumentCaptor<List<String>> caListCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockMqttClientKeyStore).updateCA(caListCaptor.capture());
        assertThat(caListCaptor.getValue(), is(Arrays.asList("CA1", "CA2")));

        caTopic = Topic.of(context, "authorities", Collections.emptyList());
        when(mockDCMConfig.lookup(MQTTBridge.RUNTIME_CONFIG_KEY, MQTTBridge.CERTIFICATES_TOPIC, MQTTBridge.AUTHORITIES))
                .thenReturn(caTopic);
        reset(mockMqttClientKeyStore);
        new MQTTBridge(config, mockTopicMapping, mockMessageBridge, mockKernel, mockMqttClientKeyStore);
        verify(mockMqttClientKeyStore, never()).updateCA(caListCaptor.capture());
    }
}
