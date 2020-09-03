package com.aws.iot.evergreen.mqtt.bridge;

import com.aws.iot.evergreen.config.Topics;
import com.aws.iot.evergreen.dependency.ImplementsService;
import com.aws.iot.evergreen.dependency.State;
import com.aws.iot.evergreen.kernel.EvergreenService;
import com.aws.iot.evergreen.mqtt.bridge.clients.MQTTClient;
import com.aws.iot.evergreen.mqtt.bridge.clients.MQTTClientException;
import com.aws.iot.evergreen.packagemanager.KernelConfigResolver;
import com.aws.iot.evergreen.util.Coerce;
import com.aws.iot.evergreen.util.Utils;
import lombok.AccessLevel;
import lombok.Getter;

import java.io.IOException;
import javax.inject.Inject;

@ImplementsService(name = MQTTBridge.SERVICE_NAME)
public class MQTTBridge extends EvergreenService {
    public static final String SERVICE_NAME = "aws.greengrass.mqtt.bridge";

    @Getter(AccessLevel.PACKAGE) // Getter for unit tests
    private final TopicMapping topicMapping;
    private final MessageBridge messageBridge;
    private MQTTClient mqttClient;
    static final String MQTT_TOPIC_MAPPING = "mqttTopicMapping";

    /**
     * Ctr for MQTTBridge.
     *
     * @param topics       topics passed by by the kernel
     * @param topicMapping mapping of mqtt topics to iotCore/pubsub topics
     */
    @Inject
    public MQTTBridge(Topics topics, TopicMapping topicMapping) {
        this(topics, new TopicMapping(), new MessageBridge(topicMapping));
    }

    protected MQTTBridge(Topics topics, TopicMapping topicMapping, MessageBridge messageBridge) {
        super(topics);
        this.topicMapping = topicMapping;

        topics.lookup(KernelConfigResolver.PARAMETERS_CONFIG_KEY, MQTT_TOPIC_MAPPING).dflt("[]")
                .subscribe((why, newv) -> {
                    try {
                        String mapping = Coerce.toString(newv);
                        if (Utils.isEmpty(mapping)) {
                            logger.debug("Mapping null or empty");
                            return;
                        }
                        topicMapping.updateMapping(mapping);
                    } catch (IOException e) {
                        logger.atError("Invalid topic mapping").kv("TopicMapping", Coerce.toString(newv)).log();
                        // Currently, kernel spills all exceptions in std err which junit consider failures
                        serviceErrored(String.format("Invalid topic mapping. %s", e.getMessage()));
                    }
                });

        this.messageBridge = messageBridge;
        try {
            this.mqttClient = new MQTTClient(topics);
        } catch (MQTTClientException e) {
            serviceErrored(e);
        }
    }

    @Override
    public void startup() {
        try {
            mqttClient.start();
            messageBridge.addOrReplaceMessageClient(TopicMapping.TopicType.LocalMqtt, mqttClient);
        } catch (MQTTClientException e) {
            serviceErrored(e);
            return;
        }
        reportState(State.RUNNING);
    }

    @Override
    public void shutdown() {
        messageBridge.removeMessageClient(TopicMapping.TopicType.LocalMqtt);
        if (mqttClient != null) {
            mqttClient.stop();
        }
    }
}
