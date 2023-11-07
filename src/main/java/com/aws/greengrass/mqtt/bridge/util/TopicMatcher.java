/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.util;

import java.nio.charset.StandardCharsets;

/**
 * Implementation adapted from <a href="https://github.com/eclipse/paho.mqtt.java/blob/f4e0db802a4433645ef011e711646a09ec9fae89/org.eclipse.paho.client.mqttv3/src/main/java/org/eclipse/paho/client/mqttv3/MqttTopic.java#L253-L321">paho</a>.
 */
@SuppressWarnings("PMD.UseVarargs")
public final class TopicMatcher {
    private static final String TOPIC_LEVEL_SEPARATOR = "/";
    private static final String MULTI_LEVEL_WILDCARD = "#";
    private static final String SINGLE_LEVEL_WILDCARD = "+";
    private static final String MULTI_LEVEL_WILDCARD_PATTERN = TOPIC_LEVEL_SEPARATOR + MULTI_LEVEL_WILDCARD;
    private static final String TOPIC_WILDCARDS = MULTI_LEVEL_WILDCARD + SINGLE_LEVEL_WILDCARD;
    private static final char[] TOPIC_WILDCARDS_ARR = TopicMatcher.TOPIC_WILDCARDS.toCharArray();
    private static final int MIN_TOPIC_LEN = 1;
    private static final int MAX_TOPIC_LEN = 65_535;
    private static final char NUL = '\u0000';
    private static final int INDEX_NOT_FOUND = -1;

    private TopicMatcher() {
    }

    /**
     * True if topic matches the given filter.
     *
     * @param topicFilter topic filter
     * @param topicName   topic name
     * @return true if topic matches the filter
     * @throws IllegalArgumentException if filter is invalid
     */
    public static boolean isMatched(String topicFilter, String topicName) {
        validate(topicFilter, true);
        validate(topicName, false);

        if (topicFilter.equals(topicName)) {
            return true;
        }

        int topicPos = 0;
        int filterPos = 0;
        int topicLen = topicName.length();
        int filterLen = topicFilter.length();
        while (filterPos < filterLen && topicPos < topicLen) {
            if (topicFilter.charAt(filterPos) == '#') {
                /*
                 * next 'if' will break when topicFilter = topic/# and topicName topic/A/,
                 * but they are matched
                 */
                topicPos = topicLen;
                filterPos = filterLen;
                break;
            }
            if (topicName.charAt(topicPos) == '/' && topicFilter.charAt(filterPos) != '/') {
                break;
            }
            if (topicFilter.charAt(filterPos) != '+' && topicFilter.charAt(filterPos) != '#'
                    && topicFilter.charAt(filterPos) != topicName.charAt(topicPos)) {
                break;
            }

            if (topicFilter.charAt(filterPos) == '+') { // skip until we meet the next separator, or end of string
                int nextpos = topicPos + 1;
                while (nextpos < topicLen && topicName.charAt(nextpos) != '/') {
                    nextpos = ++topicPos + 1;
                }
            }

            filterPos++;
            topicPos++;
        }

        if (topicPos == topicLen && filterPos == filterLen) {
            return true;
        } else {
            /*
             * https://github.com/eclipse/paho.mqtt.java/issues/418
             * Covers edge case to match sport/# to sport
             */
            if (topicFilter.length() - filterPos > 0 && topicPos == topicLen) {
                if (topicName.charAt(topicPos - 1) == '/' && topicFilter.charAt(filterPos) == '#') {
                    return true;
                }
                return topicFilter.length() - filterPos > 1
                        && topicFilter.startsWith("/#", filterPos);
            }
        }
        return false;
    }

    static void validate(String topicString, boolean wildcardAllowed) {
        int topicLen = topicString.getBytes(StandardCharsets.UTF_8).length;

        // Spec: length check
        // - All Topic Names and Topic Filters MUST be at least one character
        // long
        // - Topic Names and Topic Filters are UTF-8 encoded strings, they MUST
        // NOT encode to more than 65535 bytes
        if (topicLen < MIN_TOPIC_LEN || topicLen > MAX_TOPIC_LEN) {
            throw new IllegalArgumentException(String.format("Invalid topic length, should be in range[%d, %d]!",
                    MIN_TOPIC_LEN, MAX_TOPIC_LEN));
        }

        // *******************************************************************************
        // 1) This is a topic filter string that can contain wildcard characters
        // *******************************************************************************
        if (wildcardAllowed) {
            // Only # or +
            if (equalsAny(topicString, new String[] { MULTI_LEVEL_WILDCARD, SINGLE_LEVEL_WILDCARD })) {
                return;
            }

            // 1) Check multi-level wildcard
            // Rule:
            // The multi-level wildcard can be specified only on its own or next
            // to the topic level separator character.

            // - Can only contains one multi-level wildcard character
            // - The multi-level wildcard must be the last character used within
            // the topic tree
            if (countMatches(topicString, MULTI_LEVEL_WILDCARD) > 1
                    || topicString.contains(MULTI_LEVEL_WILDCARD)
                    && !topicString.endsWith(MULTI_LEVEL_WILDCARD_PATTERN)) {
                throw new IllegalArgumentException(
                        "Invalid usage of multi-level wildcard in topic string: " + topicString);
            }

            // 2) Check single-level wildcard
            // Rule:
            // The single-level wildcard can be used at any level in the topic
            // tree, and in conjunction with the
            // multilevel wildcard. It must be used next to the topic level
            // separator, except when it is specified on
            // its own.
            validateSingleLevelWildcard(topicString);

            return;
        }

        // *******************************************************************************
        // 2) This is a topic name string that MUST NOT contains any wildcard characters
        // *******************************************************************************
        if (containsAny(topicString, TOPIC_WILDCARDS_ARR)) {
            throw new IllegalArgumentException("The topic name MUST NOT contain any wildcard characters (#+)");
        }
    }

    @SuppressWarnings("PMD.CollapsibleIfStatements")
    private static void validateSingleLevelWildcard(String topicString) {
        char singleLevelWildcardChar = SINGLE_LEVEL_WILDCARD.charAt(0);
        char topicLevelSeparatorChar = TOPIC_LEVEL_SEPARATOR.charAt(0);

        char[] chars = topicString.toCharArray();
        int length = chars.length;
        char prev;
        char next;
        for (int i = 0; i < length; i++) {
            prev = i - 1 >= 0 ? chars[i - 1] : NUL;
            next = i + 1 < length ? chars[i + 1] : NUL;

            if (chars[i] == singleLevelWildcardChar) {
                // prev and next can be only '/' or none
                if (prev != topicLevelSeparatorChar && prev != NUL || next != topicLevelSeparatorChar && next != NUL) {
                    throw new IllegalArgumentException(
                            String.format("Invalid usage of single-level wildcard in topic string '%s'!",
                                    topicString));
                }
            }
        }
    }

    private static boolean equalsAny(CharSequence cs, CharSequence[] strs) {
        boolean eq = false;
        if (cs == null) {
            eq = strs == null;
        }

        if (strs != null) {
            for (CharSequence str : strs) {
                eq = eq || str.equals(cs);
            }
        }

        return eq;
    }

    private static boolean containsAny(CharSequence cs, char[] searchChars) {
        if (isEmpty(cs) || isEmpty(searchChars)) {
            return false;
        }
        int csLength = cs.length();
        int searchLength = searchChars.length;
        int csLast = csLength - 1;
        int searchLast = searchLength - 1;
        for (int i = 0; i < csLength; i++) {
            char ch = cs.charAt(i);
            for (int j = 0; j < searchLength; j++) {
                if (searchChars[j] == ch) {
                    if (Character.isHighSurrogate(ch)) {
                        if (j == searchLast) {
                            // missing low surrogate, fine, like String.indexOf(String)
                            return true;
                        }
                        if (i < csLast && searchChars[j + 1] == cs.charAt(i + 1)) {
                            return true;
                        }
                    } else {
                        // ch is in the Basic Multilingual Plane
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @SuppressWarnings("PMD.AssignmentInOperand")
    private static int countMatches(CharSequence str, CharSequence sub) {
        if (isEmpty(str) || isEmpty(sub)) {
            return 0;
        }
        int count = 0;
        int idx = 0;
        while ((idx = indexOf(str, sub, idx)) != INDEX_NOT_FOUND) {
            count++;
            idx += sub.length();
        }
        return count;
    }

    private static int indexOf(CharSequence cs, CharSequence searchChar, int start) {
        return cs.toString().indexOf(searchChar.toString(), start);
    }

    public static boolean isEmpty(CharSequence cs) {
        return cs == null || cs.length() == 0;
    }

    private static boolean isEmpty(char[] array) {
        return array == null || array.length == 0;
    }
}
