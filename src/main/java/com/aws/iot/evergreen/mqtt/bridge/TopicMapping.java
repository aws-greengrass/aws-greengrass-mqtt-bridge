/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.iot.evergreen.mqtt.bridge;

import com.aws.iot.evergreen.util.SerializerFactory;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Topic mappings from mqtt topic to other topics (iot core or pub sub).
 */
@NoArgsConstructor
public class TopicMapping {
    @Getter(AccessLevel.PACKAGE)
    private List<MappingEntry> mapping = new ArrayList<>();

    /**
     * Type of the mapped topic.
     */
    public enum MappedTopicType {
        IotCore, Pubsub
    }

    /**
     * A single entry in the mapping.
     */
    @AllArgsConstructor
    @NoArgsConstructor
    @EqualsAndHashCode
    public static class MappingEntry {
        @Getter
        @JsonProperty("MqttTopic")
        private String mqttTopic;
        @Getter
        @JsonProperty("MappedTopic")
        private String mappedTopic;
        @Getter
        @JsonProperty("Type")
        private MappedTopicType type;
    }

    /**
     * Update the topic mapping by parsing the mapping given as json.
     *
     * @param mappingAsJson mapping as a json string
     * @throws JsonProcessingException if unable to parse the string
     */
    public void updateMapping(@NonNull String mappingAsJson) throws JsonProcessingException {
        final TypeReference<ArrayList<MappingEntry>> typeRef = new TypeReference<ArrayList<MappingEntry>>() {
        };
        mapping = SerializerFactory.getJsonObjectMapper().readValue(mappingAsJson, typeRef);
        // TODO: Check for duplicates?
    }

    /**
     * Get mappings of mqtt topic <-> topic of provided {@link MappedTopicType} type.
     *
     * @param type {@link MappedTopicType}
     * @return topic mapping
     */
    public List<MappingEntry> getMappingsOfType(MappedTopicType type) {
        return mapping.stream().filter(entry -> entry.type.equals(type)).collect(Collectors.toList());
    }
}
