/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sdk.sample.helloworld.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseListener;
import org.opensearch.common.Strings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.jobscheduler.JobSchedulerPlugin;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.jobscheduler.transport.AcquireLockRequest;
import org.opensearch.jobscheduler.transport.AcquireLockResponse;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.SDKClient;
import org.opensearch.sdk.sample.helloworld.schedule.GreetJob;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Hello World Job Runner
 */
public class HelloWorldJobRunner implements ScheduledJobRunner {
    private static final Logger log = LogManager.getLogger(HelloWorldJobRunner.class);

    private static HelloWorldJobRunner INSTANCE;

    private SDKClient.SDKRestClient sdkRestClient;

    /**
     *
     * @return Return or create an instance of this job runner
     */
    public static HelloWorldJobRunner getJobRunnerInstance() {
        if (INSTANCE != null) {
            return INSTANCE;
        }
        synchronized (HelloWorldJobRunner.class) {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            INSTANCE = new HelloWorldJobRunner();
            return INSTANCE;
        }
    }

    public void setClient(SDKClient.SDKRestClient sdkRestClient) {
        this.sdkRestClient = sdkRestClient;
    }

    @Override
    public void runJob(ScheduledJobParameter job, JobExecutionContext context) {
        if (job.getLockDurationSeconds() != null) {
            try {
                LockModel lock = acquireLock(context, job.getLockDurationSeconds());

                if (lock == null) {
                    return;
                }

                int i = 30;
                while (i > 0) {
                    System.out.println("Remaining: " + i + " seconds");
                    try {
                        i--;
                        Thread.sleep(1000L);    // 1000L = 1000ms = 1 second
                    }
                    catch (InterruptedException ignore) { }
                }
                releaseLock(job, lock);
            } catch (Exception e) {
                log.error("Exception caught during countdown", e);
            }
        }
    }

    private LockModel acquireLock(JobExecutionContext context, Long lockDurationSeconds) throws Exception {
        // Build request body
        AcquireLockRequest acquireLockRequestBody = new AcquireLockRequest(
                context.getJobId(),
                context.getJobIndexName(),
                lockDurationSeconds
        );

        // Create acquire lock request
        Request acquireLockRequest = new Request("GET", String.format(Locale.ROOT, "%s/%s", JobSchedulerPlugin.JS_BASE_URI, "_lock"));
        acquireLockRequest
                .setJsonEntity(Strings.toString(acquireLockRequestBody.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)));

        CompletableFuture<Response> acquireLockResponse = new CompletableFuture<>();
        sdkRestClient.performRequestAsync(acquireLockRequest, new ResponseListener() {

            @Override
            public void onSuccess(Response response) {
                acquireLockResponse.complete(response);
            }

            @Override
            public void onFailure(Exception exception) {
                acquireLockResponse.completeExceptionally(exception);
            }

        });
        Response response = acquireLockResponse
                .orTimeout(5000L, TimeUnit.MILLISECONDS)
                .join();

        log.info("Acquired lock for HW job {}", context.getJobId());

        XContentParser parser = XContentType.JSON
                .xContent()
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.getEntity().getContent());
        AcquireLockResponse acquireLockResponseBody = AcquireLockResponse.parse(parser);

        return acquireLockResponseBody.getLock();
    }

    private void releaseLock(ScheduledJobParameter jobParameter, LockModel lock) {
        Request releaseLockRequest = new Request(
                "PUT",
                String.format(Locale.ROOT, "%s/%s/%s", JobSchedulerPlugin.JS_BASE_URI, "_release_lock", lock.getLockId())
        );

        try {
            CompletableFuture<Response> releaseLockResponse = new CompletableFuture<>();
            sdkRestClient.performRequestAsync(releaseLockRequest, new ResponseListener() {

                @Override
                public void onSuccess(Response response) {
                    releaseLockResponse.complete(response);
                }

                @Override
                public void onFailure(Exception exception) {
                    releaseLockResponse.completeExceptionally(exception);
                }

            });
            Response response = releaseLockResponse
                    .orTimeout(5000L, TimeUnit.MILLISECONDS)
                    .join();

            boolean lockIsReleased = RestStatus.fromCode(response.getStatusLine().getStatusCode()) == RestStatus.OK ? true : false;
            if (lockIsReleased) {
                log.info("Released lock for AD job {}", jobParameter.getName());
            } else {
                throw new IOException("Failed to release lock");
            }
        } catch (IOException exception) {
            log.error("Failed to release lock for AD job: " + jobParameter.getName(), exception);
        }
    }
}
