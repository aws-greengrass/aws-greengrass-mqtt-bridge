/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.util;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class FatalErrorHandler {

    public final AtomicReference<Exception> fatalClientError = new AtomicReference<>();
    private Consumer<Exception> callback;

    public void initialize(Consumer<Exception> callback) {
        this.fatalClientError.set(null);
        this.callback = callback;
    }

    public boolean fatalErrorOccurred() {
        return fatalClientError.get() != null;
    }

    public void fatalError(Exception e) {
        fatalClientError.set(e);
        if (callback != null) {
            callback.accept(e);
        }
    }
}
