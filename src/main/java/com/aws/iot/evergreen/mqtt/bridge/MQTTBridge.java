package com.aws.iot.evergreen.mqtt.bridge;

import com.aws.iot.evergreen.builtin.services.pubsub.PubSubIPCAgent;
import com.aws.iot.evergreen.config.Topics;
import com.aws.iot.evergreen.dependency.ImplementsService;
import com.aws.iot.evergreen.dependency.State;
import com.aws.iot.evergreen.kernel.EvergreenService;
import com.aws.iot.evergreen.mqtt.MqttClient;
import com.aws.iot.evergreen.mqtt.bridge.clients.IoTCoreClient;
import com.aws.iot.evergreen.mqtt.bridge.clients.MQTTClient;
import com.aws.iot.evergreen.mqtt.bridge.clients.MQTTClientException;
import com.aws.iot.evergreen.mqtt.bridge.clients.PubSubClient;
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
    private PubSubClient pubSubClient;
    private IoTCoreClient ioTCoreClient;
    static final String MQTT_TOPIC_MAPPING = "mqttTopicMapping";

    /**
     * Ctr for MQTTBridge.
     *
     * @param topics         topics passed by by the kernel
     * @param topicMapping   mapping of mqtt topics to iotCore/pubsub topics
     * @param pubSubIPCAgent IPC agent for pubsub
     * @param mqttClient     mqtt client for iot core
     */
    @Inject
    public MQTTBridge(Topics topics, TopicMapping topicMapping, PubSubIPCAgent pubSubIPCAgent, MqttClient mqttClient) {
        this(topics, topicMapping, new MessageBridge(topicMapping), pubSubIPCAgent, mqttClient);
    }

    protected MQTTBridge(Topics topics, TopicMapping topicMapping, MessageBridge messageBridge,
                         PubSubIPCAgent pubSubIPCAgent, MqttClient mqttClient) {
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
                        logger.atDebug().kv("mapping", mapping).log("Updating mapping");
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
        this.pubSubClient = new PubSubClient(pubSubIPCAgent);
        this.ioTCoreClient = new IoTCoreClient(mqttClient);
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
        pubSubClient.start();
        messageBridge.addOrReplaceMessageClient(TopicMapping.TopicType.Pubsub, pubSubClient);

        ioTCoreClient.start();
        messageBridge.addOrReplaceMessageClient(TopicMapping.TopicType.IotCore, ioTCoreClient);

        reportState(State.RUNNING);
    }

    @Override
    public void shutdown() {
        messageBridge.removeMessageClient(TopicMapping.TopicType.LocalMqtt);
        if (mqttClient != null) {
            mqttClient.stop();
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
}
