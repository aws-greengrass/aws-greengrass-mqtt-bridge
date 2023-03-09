/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;

@RequiredArgsConstructor
public enum MqttVersion {

    MQTT5("mqtt5"),
    MQTT3("mqtt3");

    @Getter
    private final String name;

    /**
     * Get an MqttVersion from its name.
     *
     * @param name mqtt version string
     * @return mqtt version
     */
    public static MqttVersion fromName(String name) {
        return Arrays.stream(MqttVersion.values())
                .filter(v -> v.getName().equalsIgnoreCase(name))
                .findFirst()
                .orElse(null);
    }

    @Override
    public String toString() {
        return name;
    }
}
