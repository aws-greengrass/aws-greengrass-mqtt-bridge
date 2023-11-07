/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.util;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Implementation adapted from <a href="https://github.com/eclipse/paho.mqtt.java/blob/f4e0db802a4433645ef011e711646a09ec9fae89/org.eclipse.paho.client.mqttv3.test/src/test/java/org/eclipse/paho/client/mqttv3/test/MqttTopicTest.java">paho</a>.
 */
class TopicMatcherTest {

    @ParameterizedTest
    @MethodSource("validTopicFilters")
    public void GIVEN_valid_topic_filter_WHEN_validate_THEN_success(String filter) {
        TopicMatcher.validate(filter, true);
    }

    @ParameterizedTest
    @MethodSource("invalidTopicFilters")
    public void GIVEN_invalid_topic_filter_WHEN_validate_THEN_exception_thrown(String filter) {
        assertThrows(IllegalArgumentException.class, () -> TopicMatcher.validate(filter, true));
    }

    @ParameterizedTest
    @MethodSource("matches")
    public void GIVEN_topic_filters_WHEN_topic_matches_THEN_return_true(String filter, String topic) {
        assertTrue(TopicMatcher.isMatched(filter, topic));
    }

    @ParameterizedTest
    @MethodSource("nonMatches")
    public void GIVEN_topic_filters_WHEN_topic_does_not_match_THEN_return_false(String filter, String topic) {
        assertFalse(TopicMatcher.isMatched(filter, topic));
    }

    public static Stream<Arguments> matches() {
        return Stream.of(
                Arguments.of("+/+", "sport/hockey"),
                Arguments.of("/+", "/sport"),
                Arguments.of("sport/tennis/player1/#", "sport/tennis/player1"),
                Arguments.of("sport/tennis/player1/#", "sport/tennis/player1/ranking"),
                Arguments.of("sport/tennis/player1/#", "sport/tennis/player1/score/wimbledon"),
                Arguments.of("sport/#", "sport"),
                Arguments.of("#", "sport/tennis/player1"),
                Arguments.of("sport/tennis/player1/#", "sport/tennis/player1//wimbledon"),
                Arguments.of("sport/+/player1/#", "sport/tennis/player1/wimbledon"),
                Arguments.of("sport/+/player1/#", "sport/soccer/player1/UEFA")
        );
    }

    public static Stream<Arguments> nonMatches() {
        return Stream.of(
                Arguments.of("+/+", "/sport"),
                Arguments.of("+/+", "a/b/c"),
                Arguments.of("/sport/+", "/sport/"),
                Arguments.of("sport/tennis/player1/#", "sport/tennis/player2"),
                Arguments.of("sport1/#", "sport2"),
                Arguments.of("sport/tennis1/player/#", "sport/tennis2/player"),
                Arguments.of("sport//tennis/player1/#", "sport/tennis/player1//wimbledon")
        );
    }

    public static Stream<Arguments> invalidTopicFilters() {
        return Stream.of(
                Arguments.of("sport/#/ball/+/aa"),
                Arguments.of("sport/+aa"),
                Arguments.of("sport+"),
                Arguments.of("sport/tennis/#/ranking"),
                Arguments.of("sport/tennis#")
        );
    }

    public static Stream<Arguments> validTopicFilters() {
        return Stream.of(
                Arguments.of("+"),
                Arguments.of("+/+"),
                Arguments.of("+/foo"),
                Arguments.of("+/tennis/#"),
                Arguments.of("foo/+"),
                Arguments.of("foo/+/bar"),
                Arguments.of("/+"),
                Arguments.of("/+/sport/+/player1"),
                Arguments.of("#"),
                Arguments.of("/#"),
                Arguments.of("sport/#"),
                Arguments.of("sport/tennis/#")
        );
    }
}
