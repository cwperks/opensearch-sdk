/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sdk;

import org.opensearch.rest.RestRequest;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.extensions.rest.RouteHandler;

import java.util.function.Function;

public class ExtensionRouteHandler extends RouteHandler {

    public ExtensionRouteHandler(
        String extensionShortName,
        String handlerName,
        RestRequest.Method method,
        String path,
        Function<RestRequest, ExtensionRestResponse> handler
    ) {
        super("extension:" + extensionShortName + "/" + handlerName, method, path, handler);
    }

    public ExtensionRouteHandler(
            String extensionShortName,
            String handlerName,
            String legacyActionName,
            RestRequest.Method method,
            String path,
            Function<RestRequest, ExtensionRestResponse> handler
    ) {
        super("extension:" + extensionShortName + "/" + handlerName, legacyActionName, method, path, handler);
    }
}
