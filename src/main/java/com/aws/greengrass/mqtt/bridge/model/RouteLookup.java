/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.model;

import com.aws.greengrass.mqtt.bridge.BridgeConfig;
import com.aws.greengrass.mqtt.bridge.TopicMapping;

import java.util.Optional;
import javax.inject.Inject;

public class RouteLookup {

    private final BridgeConfigReference config;

    @Inject
    public RouteLookup(BridgeConfigReference config) {
        this.config = config;
    }

    public Optional<Boolean> noLocal(String route, TopicMapping.TopicType source) {
        return getMqtt5RouteOptions(route, source).map(Mqtt5RouteOptions::isNoLocal);
    }

    public Optional<Boolean> retainAsPublished(String route, TopicMapping.TopicType source) {
        return getMqtt5RouteOptions(route, source).map(Mqtt5RouteOptions::isRetainAsPublished);
    }

    private Optional<Mqtt5RouteOptions> getMqtt5RouteOptions(String route, TopicMapping.TopicType source) {
        return Optional.ofNullable(config.get())
                .filter(c -> c.getMqttVersion() == MqttVersion.MQTT5)
                .filter(c -> isSourceInTopicMapping(c, route, source))
                .map(BridgeConfig::getMqtt5RouteOptions)
                .map(opts -> opts.get(route));
    }

    private static boolean isSourceInTopicMapping(BridgeConfig conf, String route, TopicMapping.TopicType source) {
        TopicMapping.MappingEntry entry = conf.getTopicMapping().get(route);
        return entry != null && entry.getSource() == source;
    }
}
