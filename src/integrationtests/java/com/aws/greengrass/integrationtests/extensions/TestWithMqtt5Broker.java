/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests.extensions;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Tells {@link IntegrationTestExtension} to spin up a Mqtt3 broker before the test starts.
 *
 * <p>NOTE: due to limitations of the junit framework, a {@link Broker}
 * argument must be defined on the test method, to inform {@link IntegrationTestExtension}
 * of what broker to spin-up.
 *
 * <p>NOTE: test class must be annotated with {@link IntegrationTest}.
 *
 * <p>Example: test with a broker
 *
 * <pre>
 * &#64;TestWithMqtt5Broker
 * void exampleTest(Broker broker) {
 *     // test
 * }
 * </pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@ParameterizedTest
@EnumSource(value = Broker.class, names = {"MQTT5"})
public @interface TestWithMqtt5Broker {
}
