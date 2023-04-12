/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sdk.sample.helloworld.util;

import static org.opensearch.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.rest.RestStatus.INTERNAL_SERVER_ERROR;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.Nullable;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.indices.InvalidIndexNameException;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestStatus;

import com.google.common.base.Throwables;

/**
 * Utility functions for REST handlers.
 */
public final class RestHandlerUtils {
    private static final Logger logger = LogManager.getLogger(RestHandlerUtils.class);

    private RestHandlerUtils() {}

    public static XContentParser createXContentParser(RestChannel channel, BytesReference bytesReference) throws IOException {
        return XContentHelper.createParser(
            channel.request().getXContentRegistry(),
            LoggingDeprecationHandler.INSTANCE,
            bytesReference,
            XContentType.JSON
        );
    }

    public static XContentParser createXContentParserFromRegistry(NamedXContentRegistry xContentRegistry, BytesReference bytesReference)
        throws IOException {
        return XContentHelper.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, bytesReference, XContentType.JSON);
    }

    public static boolean isExceptionCausedByInvalidQuery(Exception ex) {
        if (!(ex instanceof SearchPhaseExecutionException)) {
            return false;
        }
        SearchPhaseExecutionException exception = (SearchPhaseExecutionException) ex;
        // If any shards return bad request and failure cause is IllegalArgumentException, we
        // consider the feature query is invalid and will not count the error in failure stats.
        for (ShardSearchFailure failure : exception.shardFailures()) {
            if (RestStatus.BAD_REQUEST != failure.status() || !(failure.getCause() instanceof IllegalArgumentException)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Wrap action listener to avoid return verbose error message and wrong 500 error to user.
     * Suggestion for exception handling in HW:
     * 1. If the error is caused by wrong input, throw IllegalArgumentException exception.
     * 2. For other errors, please use OpenSearchStatusException.
     *
     * TODO: tune this function for wrapped exception, return root exception error message
     *
     * @param actionListener action listener
     * @param generalErrorMessage general error message
     * @param <T> action listener response type
     * @return wrapped action listener
     */
    public static <T> ActionListener wrapRestActionListener(ActionListener<T> actionListener, String generalErrorMessage) {
        return ActionListener.<T>wrap(r -> { actionListener.onResponse(r); }, e -> {
            logger.error("Wrap exception before sending back to user", e);
            Throwable cause = Throwables.getRootCause(e);
            if (isProperExceptionToReturn(e)) {
                actionListener.onFailure(e);
            } else if (isProperExceptionToReturn(cause)) {
                actionListener.onFailure((Exception) cause);
            } else {
                RestStatus status = isBadRequest(e) ? BAD_REQUEST : INTERNAL_SERVER_ERROR;
                String errorMessage = generalErrorMessage;
                if (isBadRequest(e)) {
                    errorMessage = e.getMessage();
                } else if (cause != null && isBadRequest(cause)) {
                    errorMessage = cause.getMessage();
                }
                actionListener.onFailure(new OpenSearchStatusException(errorMessage, status));
            }
        });
    }

    public static boolean isBadRequest(Throwable e) {
        if (e == null) {
            return false;
        }
        return e instanceof IllegalArgumentException;
    }

    public static boolean isProperExceptionToReturn(Throwable e) {
        if (e == null) {
            return false;
        }
        return e instanceof OpenSearchStatusException || e instanceof IndexNotFoundException || e instanceof InvalidIndexNameException;
    }

    private static String coalesceToEmpty(@Nullable String s) {
        return s == null ? "" : s;
    }
}
