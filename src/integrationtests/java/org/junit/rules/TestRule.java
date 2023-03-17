/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.junit.rules;

// testcontainers still has some dependencies on junit4.
// we exclude junit4 from testcontainers in pom.xml, but
// it still looks for this class at runtime.
// once testcontainers is version 2.x, we can remove this.
public interface TestRule {
}
