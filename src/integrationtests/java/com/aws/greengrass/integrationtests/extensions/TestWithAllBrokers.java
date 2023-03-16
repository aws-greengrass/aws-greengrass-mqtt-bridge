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
 * An alias for {@link ParameterizedTest}, with a test for
 * each value of {@link Broker}.  The given broker is started
 * up before the test starts.
 *
 * <p>NOTE: due to limitations of the junit framework, a {@link Broker}
 * argument must be defined on the test method, to inform {@link IntegrationTestExtension}
 * of what broker to spin-up.
 *
 * <p>NOTE: test class must be annotated with {@link IntegrationTest}.
 *
 * <p>Example: test with a mqtt3 broker
 *
 * <pre>
 * &#64;TestWithAllBrokers
 * &#64;WithKernel("config.yaml")
 * void exampleTest(Broker broker) {
 *     // test
 * }
 * </pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@ParameterizedTest
@EnumSource(Broker.class)
public @interface TestWithAllBrokers {
}
