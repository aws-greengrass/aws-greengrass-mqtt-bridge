package com.aws.greengrass.integrationtests;

import com.aws.greengrass.integrationtests.extensions.BridgeIntegrationTest;
import com.aws.greengrass.integrationtests.extensions.BridgeIntegrationTestContext;
import com.aws.greengrass.integrationtests.extensions.Broker;
import com.aws.greengrass.integrationtests.extensions.TestWithMqtt5Broker;
import com.aws.greengrass.integrationtests.extensions.WithKernel;
import com.aws.greengrass.mqtt.bridge.auth.MQTTClientKeyStore;
import com.aws.greengrass.mqtt.bridge.clients.LocalMqtt5Client;
import com.aws.greengrass.testcommons.testutilities.TestUtils;
import org.mockito.Mock;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

@BridgeIntegrationTest
public class LocalMqtt5ClientTest {

    BridgeIntegrationTestContext context;

    @TestWithMqtt5Broker
    @WithKernel("mqtt5_config.yaml")
    void GIVEN_mqtt5_broker_WHEN_mqtt5_message_sent_THEN_mqtt5_message_received(Broker broker) {



        Set<String> topics = new HashSet<>();
        topics.add("mqtt/topic");
        topics.add("mqtt/topic2");
        client.updateSubscriptions(topics, );
    }
}
