/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

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

        @Override
        public String toString() {
            return String.format("{topic: %s, source: %s, target: %s}", topic, source, target);
        }
    }

    @FunctionalInterface
    public interface UpdateListener {
        void onUpdate();
    }

    /**
     * Update the topic mapping.
     *
     * @param mapping mapping to update
     */
    public void updateMapping(@NonNull Map<String, MappingEntry> mapping) {
        // TODO: Check for duplicates, General validation + unit tests. Topic strings need to be validated (allowed
        //  filter?, etc)
        this.mapping = mapping;
        updateListeners.forEach(UpdateListener::onUpdate);
    }

    public void listenToUpdates(UpdateListener listener) {
        updateListeners.add(listener);
    }
}
