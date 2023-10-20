/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests.extensions;

import com.aws.greengrass.mqtt.bridge.model.MqttVersion;
import org.junit.jupiter.api.Disabled;

import java.net.Socket;
import java.util.function.Supplier;

import static com.github.grantwest.eventually.EventuallyLambdaMatcher.eventuallyEval;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled("Only useful to run when modifying classes in this package")
public class BridgeIntegrationTestExtensionTest {

    @BridgeIntegrationTest(
            withConfig = "config.yaml",
            withBrokers = {Broker.MQTT5},
            withLocalClientVersions = {MqttVersion.MQTT3, MqttVersion.MQTT5})
    void GIVEN_v5_broker_and_any_client_version_WHEN_test_starts_THEN_bridge_connects(BridgeIntegrationTestContext context) {
        assertNotNull(context.getBroker());
        assertNotNull(context.getBrokerHost());
        assertNotNull(context.getBrokerTCPPort());
        assertNotNull(context.getKernel());
        assertNotNull(context.getRootDir());
    }

    @BridgeIntegrationTest(withConfig = "config.yaml", withBrokers = Broker.MQTT5)
    void GIVEN_mqtt5_broker_WHEN_test_starts_THEN_bridge_connects(BridgeIntegrationTestContext context) {
        assertNotNull(context.getBroker());
        assertNotNull(context.getBrokerHost());
        assertNotNull(context.getBrokerTCPPort());
        assertNotNull(context.getKernel());
        assertNotNull(context.getRootDir());
    }

    @BridgeIntegrationTest(withConfig = "config.yaml", withBrokers = Broker.MQTT3)
    void GIVEN_mqtt3_broker_WHEN_test_starts_THEN_bridge_connects(BridgeIntegrationTestContext context) {
        assertNotNull(context.getBroker());
        assertNotNull(context.getBrokerHost());
        assertNotNull(context.getBrokerTCPPort());
        assertNotNull(context.getKernel());
        assertNotNull(context.getRootDir());
    }

    @BridgeIntegrationTest(withConfig = "config.yaml", withBrokers = {Broker.MQTT3, Broker.MQTT5})
    void GIVEN_any_broker_WHEN_test_starts_THEN_bridge_connects(BridgeIntegrationTestContext context) {
        assertNotNull(context.getBroker());
        assertNotNull(context.getBrokerHost());
        assertNotNull(context.getBrokerTCPPort());
        assertNotNull(context.getKernel());
        assertNotNull(context.getRootDir());
    }

    @BridgeIntegrationTest
    void GIVEN_no_broker_and_no_kernel_WHEN_test_executed_THEN_nothing_happens(BridgeIntegrationTestContext context) {
        assertNull(context.getBroker());
        assertNull(context.getBrokerHost());
        assertNull(context.getBrokerTCPPort());
        assertNull(context.getKernel());
        assertNotNull(context.getRootDir());
    }

    @BridgeIntegrationTest(withConfig = "config.yaml")
    void GIVEN_kernel_and_no_broker_WHEN_test_starts_THEN_kernel_starts(BridgeIntegrationTestContext context) {
        assertNull(context.getBroker());
        assertNull(context.getBrokerHost());
        assertNull(context.getBrokerTCPPort());
        assertNotNull(context.getKernel());
        assertNotNull(context.getRootDir());
    }

    @BridgeIntegrationTest(withBrokers = Broker.MQTT3)
    void GIVEN_broker_and_no_kernel_WHEN_test_starts_THEN_broker_starts(BridgeIntegrationTestContext context) {
        assertNotNull(context.getBroker());
        assertNotNull(context.getBrokerHost());
        assertNotNull(context.getBrokerTCPPort());
        assertNull(context.getKernel());
        assertNotNull(context.getRootDir());
    }

    @BridgeIntegrationTest(withConfig = "config.yaml", withBrokers = {Broker.MQTT3, Broker.MQTT5})
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    void GIVEN_any_broker_WHEN_test_restarts_broker_THEN_test_executes(BridgeIntegrationTestContext context) {
        Supplier<Boolean> brokerIsListening = () -> {
            try (Socket ignored = new Socket("localhost", context.getBrokerTCPPort())) {
                return true;
            } catch (Exception e) {
                return false;
            }
        };
        assertTrue(brokerIsListening.get());
        context.stopBroker();
        assertThat("broker stopped listening", brokerIsListening, eventuallyEval(is(false)));
        assertThat("local client is disconnected", () -> context.getLocalV3Client().getMqttClientInternal().isConnected(), eventuallyEval(is(false)));
        context.startBroker();
        assertThat("broker is listening", brokerIsListening, eventuallyEval(is(true)));
        assertThat("local client reconnects", () -> context.getLocalV3Client().getMqttClientInternal().isConnected(), eventuallyEval(is(true)));
    }
}
