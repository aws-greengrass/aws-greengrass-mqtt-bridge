/* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0 */

package com.aws.iot.evergreen.mqtt.bridge;

import com.aws.iot.evergreen.testcommons.testutilities.EGExtension;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

@ExtendWith({MockitoExtension.class, EGExtension.class})
public class TopicMappingTest {

    @Test
    void GIVEN_mapping_as_json_string_WHEN_updateMapping_THEN_mapping_updated_successfully() throws Exception {
        TopicMapping mapping = new TopicMapping();
        mapping.updateMapping("[\n"
                + "  {\"SourceTopic\": \"mqtt/topic\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/cloud/topic\", \"DestTopicType\": \"IotCore\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic2\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/pubsub/topic\", \"DestTopicType\": \"Pubsub\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic3\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/cloud/topic2\", \"DestTopicType\": \"IotCore\"}\n"
                + "]");
        List<TopicMapping.MappingEntry> expectedMapping = new ArrayList<>();
        expectedMapping.add(new TopicMapping.MappingEntry("mqtt/topic", TopicMapping.TopicType.LocalMqtt,
                "/test/cloud" + "/topic", TopicMapping.TopicType.IotCore));
        expectedMapping.add(new TopicMapping.MappingEntry("mqtt/topic2", TopicMapping.TopicType.LocalMqtt,
                "/test/pubsub/topic", TopicMapping.TopicType.Pubsub));
        expectedMapping.add(new TopicMapping.MappingEntry("mqtt/topic3", TopicMapping.TopicType.LocalMqtt,
                "/test/cloud/topic2", TopicMapping.TopicType.IotCore));

        assertArrayEquals(expectedMapping.toArray(), mapping.getMapping().toArray());
    }

    @Test
    void GIVEN_invalid_mapping_as_json_string_WHEN_updateMapping_THEN_mapping_not_updated() throws Exception {
        TopicMapping mapping = new TopicMapping();
        assertThat(mapping.getMapping().size(), is(equalTo(0)));
        // Updating with invalid mapping (Providing type as Pubsub-Invalid)
        Assertions.assertThrows(JsonProcessingException.class, () -> mapping.updateMapping("[\n"
                + "  {\"SourceTopic\": \"mqtt/topic\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/cloud/topic\", \"DestTopicType\": \"IotCore\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic2\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": "
                + "\"/test/pubsub/topic\", \"DestTopicType\": \"Pubsub-Invalid\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic3\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/cloud/topic2\", \"DestTopicType\": \"IotCore\"}\n"
                + "]"));
        assertThat(mapping.getMapping().size(), is(equalTo(0)));
    }

    @Test
    void GIVEN_null_mapping_as_json_string_WHEN_updateMapping_THEN_NPE_thrown() throws Exception {
        TopicMapping mapping = new TopicMapping();
        assertThat(mapping.getMapping().size(), is(equalTo(0)));
        Assertions.assertThrows(NullPointerException.class, () -> mapping.updateMapping(null));
        assertThat(mapping.getMapping().size(), is(equalTo(0)));
    }
}
