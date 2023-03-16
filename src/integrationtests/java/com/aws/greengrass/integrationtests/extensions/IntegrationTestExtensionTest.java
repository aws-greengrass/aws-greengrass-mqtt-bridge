/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests.extensions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@IntegrationTest
public class IntegrationTestExtensionTest {

    IntegrationTestContext context;

    @TestWithMqtt5Broker
    @WithKernel("config.yaml")
    void GIVEN_mqtt5_broker_WHEN_test_starts_THEN_bridge_connects(Broker broker) {
        assertNotNull(context.getBroker());
        assertNotNull(context.getBrokerHost());
        assertNotNull(context.getBrokerPort());
        assertNotNull(context.getKernel());
        assertNotNull(context.getRootDir());
    }

    @TestWithMqtt3Broker
    @WithKernel("config.yaml")
    void GIVEN_mqtt3_broker_WHEN_test_starts_THEN_bridge_connects(Broker broker) {
        assertNotNull(context.getBroker());
        assertNotNull(context.getBrokerHost());
        assertNotNull(context.getBrokerPort());
        assertNotNull(context.getKernel());
        assertNotNull(context.getRootDir());
    }

    @TestWithAllBrokers
    @WithKernel("config.yaml")
    void GIVEN_any_broker_WHEN_test_starts_THEN_bridge_connects(Broker broker) {
        assertNotNull(context.getBroker());
        assertNotNull(context.getBrokerHost());
        assertNotNull(context.getBrokerPort());
        assertNotNull(context.getKernel());
        assertNotNull(context.getRootDir());
    }

    @ParameterizedTest
    @ValueSource(strings = "unused")
    void GIVEN_no_broker_and_no_kernel_WHEN_parameterized_test_executed_THEN_nothing_happens(String unused) {
        assertNull(context.getBroker());
        assertNull(context.getBrokerHost());
        assertNull(context.getBrokerPort());
        assertNull(context.getKernel());
        assertNotNull(context.getRootDir());
    }
    @Test
    void GIVEN_no_broker_and_no_kernel_WHEN_test_executed_THEN_nothing_happens() {
        assertNull(context.getBroker());
        assertNull(context.getBrokerHost());
        assertNull(context.getBrokerPort());
        assertNull(context.getKernel());
        assertNotNull(context.getRootDir());
    }

    @Test
    @WithKernel("config.yaml")
    void GIVEN_kernel_and_no_broker_WHEN_test_starts_THEN_kernel_starts() {
        assertNull(context.getBroker());
        assertNull(context.getBrokerHost());
        assertNull(context.getBrokerPort());
        assertNotNull(context.getKernel());
        assertNotNull(context.getRootDir());
    }

    @ParameterizedTest
    @ValueSource(strings = "unused")
    @WithKernel("config.yaml")
    void GIVEN_kernel_and_no_broker_WHEN_parameterized_test_starts_THEN_kernel_starts(String unused) {
        assertNull(context.getBroker());
        assertNull(context.getBrokerHost());
        assertNull(context.getBrokerPort());
        assertNotNull(context.getKernel());
        assertNotNull(context.getRootDir());
    }

    @TestWithMqtt3Broker
    void GIVEN_broker_and_no_kernel_WHEN_test_starts_THEN_broker_starts(Broker broker) {
        assertNotNull(context.getBroker());
        assertNotNull(context.getBrokerHost());
        assertNotNull(context.getBrokerPort());
        assertNull(context.getKernel());
        assertNotNull(context.getRootDir());
    }
}
