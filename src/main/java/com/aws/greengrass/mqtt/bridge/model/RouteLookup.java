/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.model;

import com.aws.greengrass.mqtt.bridge.BridgeConfig;
import com.aws.greengrass.mqtt.bridge.TopicMapping;
import lombok.NonNull;

import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;

public class RouteLookup {

    private final BridgeConfigReference config;

    @Inject
    public RouteLookup(BridgeConfigReference config) {
        this.config = config;
    }

    public Optional<Boolean> noLocal(String topic, TopicMapping.TopicType source) {
        return getMqtt5RouteOptions(topic, source).map(Mqtt5RouteOptions::isNoLocal);
    }

    private Optional<Mqtt5RouteOptions> getMqtt5RouteOptions(String topic, TopicMapping.TopicType source) {
        return Optional.ofNullable(config.get())
                .filter(c -> c.getMqttVersion() == MqttVersion.MQTT5)
                .flatMap(c -> getMqtt5RouteOptions(c, topic, source));
    }

    private static Optional<Mqtt5RouteOptions> getMqtt5RouteOptions(@NonNull BridgeConfig conf,
                                                                    @NonNull String topic,
                                                                    @NonNull TopicMapping.TopicType source) {
        Map<String, Mqtt5RouteOptions> options = conf.getMqtt5RouteOptions();
        return findRouteName(conf, topic, source).map(options::get);
    }

    private static Optional<String> findRouteName(BridgeConfig conf, String topic, TopicMapping.TopicType source) {
        return conf.getTopicMapping().entrySet().stream()
                .filter(e -> e.getValue() != null
                        && e.getValue().getSource() == source
                        && e.getValue().getTopic().equals(topic))
                .map(Map.Entry::getKey)
                .findFirst();
    }
}
