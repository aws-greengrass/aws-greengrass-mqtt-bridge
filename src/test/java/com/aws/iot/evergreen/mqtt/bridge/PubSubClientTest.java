package com.aws.iot.evergreen.mqtt.bridge;

import com.aws.iot.evergreen.builtin.services.pubsub.PubSubIPCAgent;
import com.aws.iot.evergreen.ipc.services.pubsub.MessagePublishedEvent;
import com.aws.iot.evergreen.ipc.services.pubsub.PubSubPublishRequest;
import com.aws.iot.evergreen.ipc.services.pubsub.PubSubSubscribeRequest;
import com.aws.iot.evergreen.testcommons.testutilities.EGExtension;
import com.aws.iot.evergreen.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.aws.iot.evergreen.testcommons.testutilities.TestUtils.asyncAssertOnConsumer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({MockitoExtension.class, EGExtension.class})
public class PubSubClientTest {
    private MessageBridge messageBridge;

    private PubSubIPCAgent pubSubIPCAgent;

    private TopicMapping mapping;

    private PubSubClient pubSubClient;

    @BeforeEach
    public void setup() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        pubSubIPCAgent = new PubSubIPCAgent(executor);
        messageBridge = new MessageBridge();
        mapping = new TopicMapping();
        pubSubClient = new PubSubClient(messageBridge, mapping, pubSubIPCAgent);
    }

    @Test
    void GIVEN_PubSubClient_WHEN_routing_config_updated_THEN_route_maps_updated() throws Exception {
        mapping.updateMapping("[\n"
                + "  {\"SourceTopic\": \"test/pubsub/topic\", \"SourceTopicType\": \"Pubsub\", \"DestTopic\": \"mqtt/topic\", \"DestTopicType\": \"LocalMqtt\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic2\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"test/pubsub/topic2\", \"DestTopicType\": \"Pubsub\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic3\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"test/cloud/topic3\", \"DestTopicType\": \"IotCore\"}\n"
                + "]");
        pubSubClient.start();

        Set<String> expectedPubSubTopics = new HashSet<>(Arrays.asList("test/pubsub/topic"));
        Map<String, List<String>> expectedMqttToPubSub = new HashMap<String, List<String>>() {{
            put("mqtt/topic2", new ArrayList<>(Arrays.asList("test/pubsub/topic2")));
        }};
        assertEquals(expectedMqttToPubSub, pubSubClient.getRouteMqttToPubSub());
        assertEquals(expectedPubSubTopics, pubSubClient.getPubSubTopics());

        //modify mapping and test again
        mapping.updateMapping("[\n"
                + "  {\"SourceTopic\": \"test/pubsub/topic\", \"SourceTopicType\": \"Pubsub\", \"DestTopic\": \"mqtt/topic\", \"DestTopicType\": \"LocalMqtt\"},\n"
                + "  {\"SourceTopic\": \"test/pubsub/topic\", \"SourceTopicType\": \"Pubsub\", \"DestTopic\": \"mqtt/topic1\", \"DestTopicType\": \"LocalMqtt\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic4\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"test/pubsub/topic4\", \"DestTopicType\": \"Pubsub\"},\n"
                + "  {\"SourceTopic\": \"test/pubsub/topic5\", \"SourceTopicType\": \"Pubsub\", \"DestTopic\": \"mqtt/topic5\", \"DestTopicType\": \"LocalMqtt\"}\n"
                + "]");
        pubSubClient.updateRoutingConfigAndSubscriptions();
        expectedPubSubTopics.add("test/pubsub/topic5");
        expectedMqttToPubSub.remove("mqtt/topic2");
        expectedMqttToPubSub.put("mqtt/topic4", new ArrayList<>(Arrays.asList("test/pubsub/topic4")));
        assertEquals(expectedMqttToPubSub, pubSubClient.getRouteMqttToPubSub());
        assertEquals(expectedPubSubTopics, pubSubClient.getPubSubTopics());

        pubSubClient.stop();
    }

    @Test
    void GIVEN_PubSubClient_WHEN_routing_config_updated_THEN_messages_communicated() throws Exception {
        mapping.updateMapping("[\n"
                + "  {\"SourceTopic\": \"test/pubsub/topic\", \"SourceTopicType\": \"Pubsub\", \"DestTopic\": \"mqtt/topic\", \"DestTopicType\": \"LocalMqtt\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic2\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"test/pubsub/topic2\", \"DestTopicType\": \"Pubsub\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic3\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"test/cloud/topic3\", \"DestTopicType\": \"IotCore\"}\n"
                + "]");
        pubSubClient.start();

        //test LocalMqtt -> Pubsub message flow
        Pair<CompletableFuture<Void>, Consumer<MessagePublishedEvent>> cb = asyncAssertOnConsumer((m) -> {
            assertEquals("some message", new String(m.getPayload(), StandardCharsets.UTF_8));
            assertEquals("test/pubsub/topic2", m.getTopic());
        });
        PubSubSubscribeRequest subscribeRequest = PubSubSubscribeRequest.builder().topic("test/pubsub/topic2").build();
        pubSubIPCAgent.subscribe(subscribeRequest, cb.getRight());

        Message message = new Message("mqtt/topic2", "some message".getBytes(StandardCharsets.UTF_8));
        messageBridge.notifyMessage(message, TopicMapping.TopicType.LocalMqtt);
        cb.getLeft().get(1, TimeUnit.SECONDS);

        //test Pubsub -> LocalMqtt message flow
        CountDownLatch listenerRan = new CountDownLatch(1);
        MessageBridge.MessageListener listener = (sourceType, msg) -> {
            assertEquals("some message", new String(msg.getPayload(), StandardCharsets.UTF_8));
            assertEquals("test/pubsub/topic", msg.getTopic());
            listenerRan.countDown();
        };
        messageBridge.addListener(listener, TopicMapping.TopicType.Pubsub);

        PubSubPublishRequest publishRequest = PubSubPublishRequest.builder().topic("test/pubsub/topic")
                .payload("some message".getBytes(StandardCharsets.UTF_8)).build();
        pubSubIPCAgent.publish(publishRequest);
        assertTrue(listenerRan.await(1, TimeUnit.SECONDS));

        pubSubClient.stop();
    }
}
