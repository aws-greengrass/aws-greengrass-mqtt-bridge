/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge;

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
import com.aws.greengrass.mqtt.bridge.clients.MessageClient;
import com.aws.greengrass.mqtt.bridge.clients.MessageClientException;
import com.aws.greengrass.mqtt.bridge.clients.MessageClients;
import com.aws.greengrass.mqtt.bridge.model.BridgeConfigReference;
import com.aws.greengrass.mqtt.bridge.model.InvalidConfigurationException;
import com.aws.greengrass.util.BatchedSubscriber;
import lombok.Getter;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.inject.Inject;

import static com.aws.greengrass.lifecyclemanager.Lifecycle.LIFECYCLE_SHUTDOWN_NAMESPACE_TOPIC;
import static com.aws.greengrass.lifecyclemanager.Lifecycle.TIMEOUT_NAMESPACE_TOPIC;


@ImplementsService(name = MQTTBridge.SERVICE_NAME)
public class MQTTBridge extends PluginService {
    public static final String SERVICE_NAME = "aws.greengrass.clientdevices.mqtt.Bridge";

    private final TopicMapping topicMapping;
    private final MessageBridge messageBridge;
    private final Kernel kernel;
    private final MQTTClientKeyStore mqttClientKeyStore;
    private final ConfigurationChangeHandler configurationChangeHandler;
    private final CertificateAuthorityChangeHandler certificateAuthorityChangeHandler;
    private final MessageClients messageClients;
    @Getter // for tests
    private final BridgeConfigReference bridgeConfig;


    /**
     * Ctr for MQTTBridge.
     *
     * @param topics                 topics passed by the Nucleus
     * @param topicMapping           mapping of mqtt topics to iotCore/pubsub topics
     * @param messageBridge          message bridge
     * @param kernel                 Greengrass kernel
     * @param mqttClientKeyStore     KeyStore for MQTT Client
     * @param messageClients         message clients
     * @param bridgeConfig           reference to bridge config
     */
    @Inject
    public MQTTBridge(Topics topics,
                      TopicMapping topicMapping,
                      MessageBridge messageBridge,
                      Kernel kernel,
                      MQTTClientKeyStore mqttClientKeyStore,
                      MessageClients messageClients,
                      BridgeConfigReference bridgeConfig) {
        super(topics);
        this.topicMapping = topicMapping;
        this.kernel = kernel;
        this.mqttClientKeyStore = mqttClientKeyStore;
        this.messageBridge = messageBridge;
        this.messageClients = messageClients;
        this.configurationChangeHandler = new ConfigurationChangeHandler();
        this.certificateAuthorityChangeHandler = new CertificateAuthorityChangeHandler();
        this.bridgeConfig = bridgeConfig;

        // Increase default service shutdown time to 120s
        Topic shutdownTimeoutTopic =
                config.lookup(SERVICE_LIFECYCLE_NAMESPACE_TOPIC, LIFECYCLE_SHUTDOWN_NAMESPACE_TOPIC,
                        TIMEOUT_NAMESPACE_TOPIC);
        shutdownTimeoutTopic.withParentNeedsToKnow(false);
        shutdownTimeoutTopic.dflt(120);
    }

    @Override
    public void install() {
        configurationChangeHandler.listen();
        try {
            messageBridge.initialize();
        } catch (MessageClientException e) {
            serviceErrored(e);
        }
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
            for (MessageClient<?> client : messageClients) {
                client.start();
            }
            reportState(State.RUNNING);
        } catch (MessageClientException e) {
            serviceErrored(e);
        }
    }

    @Override
    public void shutdown() {
        certificateAuthorityChangeHandler.stop();
        mqttClientKeyStore.shutdown();
        messageClients.forEach(MessageClient::stop);
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
                    .flatMap(this::caTopicToPojo)
                    .ifPresent(this::updateCA);
        }

        @SuppressWarnings("unchecked")
        private Optional<List<String>> caTopicToPojo(Topic topic) {
            Object pojo = topic.toPOJO();
            if (pojo == null) {
                logger.atWarn().log("CDA CA topic not set");
                return Optional.empty();
            }

            if (pojo instanceof List) {
                return Optional.of((List<String>) pojo);
            }

            logger.atWarn().log("CDA CA topic malformed, expected a list but got: " + pojo.getClass());
            return Optional.empty();
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
                        .locate(ClientDevicesAuthService.CLIENT_DEVICES_AUTH_SERVICE_NAME)
                        .getRuntimeConfig()
                        .lookup(
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
            BridgeConfig newConfig;
            try {
                newConfig = BridgeConfig.fromTopics(configurationTopics);
            } catch (InvalidConfigurationException e) {
                serviceErrored(e);
                return;
            }

            BridgeConfig prevConfig = bridgeConfig.getAndSet(newConfig);

            // update topic mapping
            if (prevConfig == null || !Objects.equals(prevConfig.getTopicMapping(), newConfig.getTopicMapping())) {
                logger.atInfo("service-config-change")
                        .kv("mapping", newConfig.getTopicMapping())
                        .log("Updating mapping");
                topicMapping.updateMapping(newConfig.getTopicMapping());
            }

            // initial config
            if (prevConfig == null) {
                return;
            }

            if (newConfig.reinstallRequired(prevConfig)) {
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
