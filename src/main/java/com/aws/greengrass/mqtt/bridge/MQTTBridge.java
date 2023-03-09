/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge;

import com.aws.greengrass.builtin.services.pubsub.PubSubIPCEventStreamAgent;
import com.aws.greengrass.clientdevices.auth.ClientDevicesAuthService;
import com.aws.greengrass.clientdevices.auth.exception.CertificateGenerationException;
import com.aws.greengrass.componentmanager.KernelConfigResolver;
import com.aws.greengrass.config.Topic;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.dependency.ImplementsService;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.lifecyclemanager.PluginService;
import com.aws.greengrass.lifecyclemanager.exceptions.ServiceLoadException;
import com.aws.greengrass.mqtt.bridge.auth.MQTTClientKeyStore;
import com.aws.greengrass.mqtt.bridge.clients.IoTCoreClient;
import com.aws.greengrass.mqtt.bridge.clients.LocalMqttClientFactory;
import com.aws.greengrass.mqtt.bridge.clients.MQTTClientException;
import com.aws.greengrass.mqtt.bridge.clients.MessageClient;
import com.aws.greengrass.mqtt.bridge.clients.PubSubClient;
import com.aws.greengrass.mqtt.bridge.model.InvalidConfigurationException;
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.util.BatchedSubscriber;
import com.aws.greengrass.util.Coerce;
import com.aws.greengrass.util.Utils;
import lombok.AccessLevel;
import lombok.Getter;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import javax.inject.Inject;


@ImplementsService(name = MQTTBridge.SERVICE_NAME)
public class MQTTBridge extends PluginService {
    public static final String SERVICE_NAME = "aws.greengrass.clientdevices.mqtt.Bridge";

    @Getter(AccessLevel.PACKAGE) // Getter for unit tests
    private final TopicMapping topicMapping;
    private final MessageBridge messageBridge;
    private final Kernel kernel;
    private final MQTTClientKeyStore mqttClientKeyStore;
    private final LocalMqttClientFactory localMqttClientFactory;
    private final ConfigurationChangeHandler configurationChangeHandler;
    private final CertificateAuthorityChangeHandler certificateAuthorityChangeHandler;
    private MessageClient localMqttClient;
    private PubSubClient pubSubClient;
    private IoTCoreClient ioTCoreClient;
    private BridgeConfig bridgeConfig;


    /**
     * Ctr for MQTTBridge.
     *
     * @param topics                 topics passed by the Nucleus
     * @param topicMapping           mapping of mqtt topics to iotCore/pubsub topics
     * @param pubSubIPCAgent         IPC agent for pubsub
     * @param iotMqttClient          mqtt client for iot core
     * @param kernel                 Greengrass kernel
     * @param mqttClientKeyStore     KeyStore for MQTT Client
     * @param localMqttClientFactory local mqtt client factory
     * @param executorService        Executor service
     */
    @Inject
    public MQTTBridge(Topics topics, TopicMapping topicMapping, PubSubIPCEventStreamAgent pubSubIPCAgent,
                      MqttClient iotMqttClient, Kernel kernel, MQTTClientKeyStore mqttClientKeyStore,
                      LocalMqttClientFactory localMqttClientFactory,
                      ExecutorService executorService) {
        this(topics, topicMapping, new MessageBridge(topicMapping), pubSubIPCAgent, iotMqttClient,
                kernel, mqttClientKeyStore, localMqttClientFactory, executorService);
    }

    protected MQTTBridge(Topics topics, TopicMapping topicMapping, MessageBridge messageBridge,
                         PubSubIPCEventStreamAgent pubSubIPCAgent, MqttClient iotMqttClient, Kernel kernel,
                         MQTTClientKeyStore mqttClientKeyStore,
                         LocalMqttClientFactory localMqttClientFactory,
                         ExecutorService executorService) {
        super(topics);
        this.topicMapping = topicMapping;
        this.kernel = kernel;
        this.mqttClientKeyStore = mqttClientKeyStore;
        this.messageBridge = messageBridge;
        this.pubSubClient = new PubSubClient(pubSubIPCAgent);
        this.ioTCoreClient = new IoTCoreClient(iotMqttClient, executorService);
        this.localMqttClientFactory = localMqttClientFactory;
        this.configurationChangeHandler = new ConfigurationChangeHandler();
        this.certificateAuthorityChangeHandler = new CertificateAuthorityChangeHandler();
    }

    @Override
    public void install() {
        configurationChangeHandler.listen();
    }

    @Override
    public void startup() {
        try {
            mqttClientKeyStore.init();
        } catch (KeyStoreException | CertificateGenerationException e) {
            serviceErrored(e);
            return;
        }

        certificateAuthorityChangeHandler.start();

        try {
            localMqttClient = localMqttClientFactory.createLocalMqttClient();
            localMqttClient.start();
            messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(
                    TopicMapping.TopicType.LocalMqtt, localMqttClient);
        } catch (MQTTClientException e) {
            serviceErrored(e);
            return;
        }
        pubSubClient.start();
        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.Pubsub, pubSubClient);

        ioTCoreClient.start();
        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.IotCore, ioTCoreClient);

        reportState(State.RUNNING);
    }

    @Override
    public void shutdown() {
        certificateAuthorityChangeHandler.stop();
        mqttClientKeyStore.shutdown();

        messageBridge.removeMessageClient(TopicMapping.TopicType.LocalMqtt);
        if (localMqttClient != null) {
            localMqttClient.stop();
        }

        messageBridge.removeMessageClient(TopicMapping.TopicType.Pubsub);
        if (pubSubClient != null) {
            pubSubClient.stop();
        }

        messageBridge.removeMessageClient(TopicMapping.TopicType.IotCore);
        if (ioTCoreClient != null) {
            ioTCoreClient.stop();
        }
    }

    public class CertificateAuthorityChangeHandler {

        private BatchedSubscriber subscriber;

        /**
         * Begin listening and responding to CDA CA changes.
         *
         * <p>This operation is idempotent.
         */
        public void start() {
            if (subscriber == null) {
                Topic caTopic = findCATopic().orElse(null);
                if (caTopic == null) {
                    return;
                }
                subscriber = new BatchedSubscriber(caTopic, (what) -> onCAChange());
            }
            subscriber.subscribe();
        }

        /**
         * Stop listening to CDA CA changes.
         */
        public void stop() {
            if (subscriber != null) {
                subscriber.unsubscribe();
            }
        }

        private void onCAChange() {
            findCATopic()
                    .map(Coerce::toStringList)
                    .filter(certs -> !Utils.isEmpty(certs))
                    .ifPresent(this::updateCA);
        }

        private void updateCA(List<String> certs) {
            logger.atDebug().kv("numCaCerts", certs.size()).log("CA update received");
            try {
                mqttClientKeyStore.updateCA(certs);
            } catch (IOException | CertificateException | KeyStoreException e) {
                serviceErrored(e);
            }
        }

        private Optional<Topic> findCATopic() {
            try {
                return Optional.of(kernel
                        .locate(ClientDevicesAuthService.CLIENT_DEVICES_AUTH_SERVICE_NAME).getConfig()
                        .lookup(RUNTIME_STORE_NAMESPACE_TOPIC,
                                ClientDevicesAuthService.CERTIFICATES_KEY,
                                ClientDevicesAuthService.AUTHORITIES_TOPIC));
            } catch (ServiceLoadException e) {
                logger.atWarn().cause(e).log(
                        "Unable to locate {}. "
                                + "MQTT Bridge may be unable to connect to the broker. "
                                + "Ensure that {} component is deployed.",
                        ClientDevicesAuthService.CLIENT_DEVICES_AUTH_SERVICE_NAME,
                        ClientDevicesAuthService.CLIENT_DEVICES_AUTH_SERVICE_NAME);
                return Optional.empty();
            }
        }
    }

    /**
     * Responsible for handling all bridge config changes.
     */
    @SuppressWarnings("PMD.PrematureDeclaration")
    public class ConfigurationChangeHandler {

        private final Topics configurationTopics = config.lookupTopics(KernelConfigResolver.CONFIGURATION_CONFIG_KEY);

        private final BatchedSubscriber subscriber = new BatchedSubscriber(configurationTopics, (what) -> {
            BridgeConfig prevConfig = bridgeConfig;
            try {
                bridgeConfig = BridgeConfig.fromTopics(configurationTopics);
            } catch (InvalidConfigurationException e) {
                serviceErrored(e);
                return;
            }

            localMqttClientFactory.setConfig(bridgeConfig);

            // update topic mapping
            if (prevConfig == null || !Objects.equals(prevConfig.getTopicMapping(), bridgeConfig.getTopicMapping())) {
                logger.atInfo("service-config-change")
                        .kv("mapping", bridgeConfig.getTopicMapping())
                        .log("Updating mapping");
                topicMapping.updateMapping(bridgeConfig.getTopicMapping());
            }

            // initial config
            if (prevConfig == null) {
                return;
            }

            if (bridgeConfig.reinstallRequired(prevConfig)) {
                logger.atInfo("service-config-change")
                        .log("Requesting re-installation of bridge");
                requestReinstall();
            }
        });

        /**
         * Begin listening and responding to bridge configuration changes.
         *
         * <p>This operation is idempotent.
         */
        public void listen() {
            subscriber.subscribe();
        }
    }
}
