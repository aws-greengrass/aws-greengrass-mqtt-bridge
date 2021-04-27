/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge;

import com.aws.greengrass.util.SerializerFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Topic mappings from mqtt topic to other topics (iot core or pub sub).
 */
@NoArgsConstructor
public class TopicMapping {
    @Getter
    private Map<String, MappingEntry> mapping = new HashMap<>();

    private List<UpdateListener> updateListeners = new CopyOnWriteArrayList<>();

    private static final ObjectMapper OBJECT_MAPPER_WITH_STRICT_DUPLICATE_KEY_DETECTION =
            SerializerFactory.getFailSafeJsonObjectMapper().copy()
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);

    /**
     * Type of the topic.
     */
    public enum TopicType {
        IotCore, Pubsub, LocalMqtt
    }

    /**
     * A single entry in the mapping.
     */
    @AllArgsConstructor
    @NoArgsConstructor
    @EqualsAndHashCode
    public static class MappingEntry {
        @Getter
        private String topic;
        @Getter
        private TopicType source;
        @Getter
        private TopicType target;
    }

    @FunctionalInterface
    public interface UpdateListener {
        void onUpdate();
    }

    /**
     * Update the topic mapping by parsing the mapping given as json.
     *
     * @param mappingAsJson mapping as a json string
     * @throws IOException if unable to parse the string
     */
    public void updateMapping(@NonNull String mappingAsJson) throws IOException {
        final TypeReference<Map<String, MappingEntry>> typeRef = new TypeReference<Map<String, MappingEntry>>() {
        };
        mapping = OBJECT_MAPPER_WITH_STRICT_DUPLICATE_KEY_DETECTION.readValue(mappingAsJson, typeRef);
        // TODO: Check for duplicates, General validation + unit tests. Topic strings need to be validated (allowed
        //  filter?, etc)
        updateListeners.forEach(UpdateListener::onUpdate);
    }

    public void listenToUpdates(UpdateListener listener) {
        updateListeners.add(listener);
    }
}
