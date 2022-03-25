/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge;

import com.aws.greengrass.componentmanager.KernelConfigResolver;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.dependency.Context;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(GGExtension.class)
class BridgeConfigTest {

    private static final String DEFAULT_BROKER_URI = "ssl://localhost:8883";
    private static final String BROKER_URI = "tcp://localhost:8883";
    private static final String BROKER_SERVER_URI = "tcp://localhost:8884";
    private static final String MALFORMED_BROKER_URI = "tcp://ma]formed.uri:8883";
    private static final String CLIENT_ID = "clientId";
    private static final String CLIENT_ID_PREFIX = "mqtt-bridge-";

    Topics topics;

    @BeforeEach
    void setUp() {
        topics = Topics.of(new Context(), KernelConfigResolver.CONFIGURATION_CONFIG_KEY, null);
    }

    @AfterEach
    void tearDown() {
        topics.getContext().shutdown();
    }

    @Test
    void GIVEN_bridgeConfig_WHEN_brokerUri_provided_THEN_uriReturned() throws URISyntaxException {
        topics.lookup(KernelConfigResolver.CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_BROKER_URI)
                .dflt(BROKER_URI);

        URI uri = BridgeConfig.getBrokerUri(topics);
        assertEquals(BROKER_URI, uri.toString());
    }

    @Test
    void GIVEN_bridgeConfig_WHEN_brokerUri_and_brokerServerUri_provided_THEN_brokerUri_returned() throws URISyntaxException {
        topics.lookup(KernelConfigResolver.CONFIGURATION_CONFIG_KEY, BridgeConfig.BROKER_SERVER_URI)
                .dflt(BROKER_SERVER_URI);
        topics.lookup(KernelConfigResolver.CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_BROKER_URI)
                .dflt(BROKER_URI);

        URI uri = BridgeConfig.getBrokerUri(topics);
        assertEquals(BROKER_URI, uri.toString());
    }

    @Test
    void GIVEN_bridgeConfig_WHEN_brokerServerUri_provided_THEN_brokerServerUri_returned() throws URISyntaxException {
        topics.lookup(KernelConfigResolver.CONFIGURATION_CONFIG_KEY, BridgeConfig.BROKER_SERVER_URI)
                .dflt(BROKER_SERVER_URI);

        URI uri = BridgeConfig.getBrokerUri(topics);
        assertEquals(BROKER_SERVER_URI, uri.toString());
    }

    @Test
    void GIVEN_bridgeConfig_WHEN_brokerUri_missing_THEN_default_brokerUri_returned() throws URISyntaxException {
        URI uri = BridgeConfig.getBrokerUri(topics);
        assertEquals(DEFAULT_BROKER_URI, uri.toString());
    }

    @Test
    void GIVEN_bridgeConfig_WHEN_brokerUri_malformed_THEN_exception_thrown() {
        topics.lookup(KernelConfigResolver.CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_BROKER_URI)
                .dflt(MALFORMED_BROKER_URI);

        assertThrows(URISyntaxException.class, () -> BridgeConfig.getBrokerUri(topics));
    }

    @Test
    void GIVEN_bridgeConfig_WHEN_clientId_provided_THEN_clientId_returned() {
        topics.lookup(KernelConfigResolver.CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_CLIENT_ID)
                .dflt(CLIENT_ID);

        String clientId = BridgeConfig.getClientId(topics);
        assertEquals(CLIENT_ID, clientId);
    }

    @Test
    void GIVEN_bridgeConfig_WHEN_clientId_missing_THEN_default_clientId_returned() {
        String clientId = BridgeConfig.getClientId(topics);
        assertTrue(clientId.startsWith(CLIENT_ID_PREFIX));
    }
}