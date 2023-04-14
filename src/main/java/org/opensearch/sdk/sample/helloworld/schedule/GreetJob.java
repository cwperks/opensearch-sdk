/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sdk.sample.helloworld.schedule;

import com.google.common.base.Objects;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.schedule.CronSchedule;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import org.opensearch.jobscheduler.spi.schedule.ScheduleParser;

import java.io.IOException;
import java.time.Instant;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class GreetJob implements Writeable, ToXContentObject, ScheduledJobParameter {
    enum ScheduleType {
        CRON,
        INTERVAL
    }

    public static final String PARSE_FIELD_NAME = "GreetJob";
    public static final NamedXContentRegistry.Entry XCONTENT_REGISTRY = new NamedXContentRegistry.Entry(
        GreetJob.class,
        new ParseField(PARSE_FIELD_NAME),
        it -> parse(it)
    );

    public static final String HELLO_WORLD_JOB_INDEX = ".hello-world-jobs";
    public static final String NAME_FIELD = "name";
    public static final String LAST_UPDATE_TIME_FIELD = "last_update_time";
    public static final String LOCK_DURATION_SECONDS = "lock_duration_seconds";

    public static final String SCHEDULE_FIELD = "schedule";
    public static final String IS_ENABLED_FIELD = "enabled";
    public static final String ENABLED_TIME_FIELD = "enabled_time";
    public static final String DISABLED_TIME_FIELD = "disabled_time";

    private final String name;
    private final Schedule schedule;
    private final Boolean isEnabled;
    private final Instant enabledTime;
    private final Instant disabledTime;
    private final Instant lastUpdateTime;
    private final Long lockDurationSeconds;

    public GreetJob(
        String name,
        Schedule schedule,
        Boolean isEnabled,
        Instant enabledTime,
        Instant disabledTime,
        Instant lastUpdateTime,
        Long lockDurationSeconds
    ) {
        this.name = name;
        this.schedule = schedule;
        this.isEnabled = isEnabled;
        this.enabledTime = enabledTime;
        this.disabledTime = disabledTime;
        this.lastUpdateTime = lastUpdateTime;
        this.lockDurationSeconds = lockDurationSeconds;
    }

    public GreetJob(StreamInput input) throws IOException {
        name = input.readString();
        if (input.readEnum(GreetJob.ScheduleType.class) == ScheduleType.CRON) {
            schedule = new CronSchedule(input);
        } else {
            schedule = new IntervalSchedule(input);
        }
        isEnabled = input.readBoolean();
        enabledTime = input.readInstant();
        disabledTime = input.readInstant();
        lastUpdateTime = input.readInstant();
        lockDurationSeconds = input.readLong();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject()
            .field(NAME_FIELD, name)
            .field(SCHEDULE_FIELD, schedule)
            .field(IS_ENABLED_FIELD, isEnabled)
            .field(ENABLED_TIME_FIELD, enabledTime.toEpochMilli())
            .field(LAST_UPDATE_TIME_FIELD, lastUpdateTime.toEpochMilli())
            .field(LOCK_DURATION_SECONDS, lockDurationSeconds);
        if (disabledTime != null) {
            xContentBuilder.field(DISABLED_TIME_FIELD, disabledTime.toEpochMilli());
        }
        return xContentBuilder.endObject();
    }

    @Override
    public void writeTo(StreamOutput output) throws IOException {
        output.writeString(name);
        if (schedule instanceof CronSchedule) {
            output.writeEnum(ScheduleType.CRON);
        } else {
            output.writeEnum(ScheduleType.INTERVAL);
        }
        schedule.writeTo(output);
        output.writeBoolean(isEnabled);
        output.writeInstant(enabledTime);
        output.writeInstant(disabledTime);
        output.writeInstant(lastUpdateTime);
        output.writeLong(lockDurationSeconds);
    }

    public static GreetJob parse(XContentParser parser) throws IOException {
        String name = null;
        Schedule schedule = null;
        // we cannot set it to null as isEnabled() would do the unboxing and results in null pointer exception
        Boolean isEnabled = Boolean.FALSE;
        Instant enabledTime = null;
        Instant disabledTime = null;
        Instant lastUpdateTime = null;
        Long lockDurationSeconds = 5L;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            System.out.println("fieldName: " + fieldName);

            switch (fieldName) {
                case NAME_FIELD:
                    name = parser.text();
                    break;
                case SCHEDULE_FIELD:
                    schedule = ScheduleParser.parse(parser);
                    break;
                case IS_ENABLED_FIELD:
                    isEnabled = parser.booleanValue();
                    break;
                case ENABLED_TIME_FIELD:
                    enabledTime = toInstant(parser);
                    break;
                case DISABLED_TIME_FIELD:
                    disabledTime = toInstant(parser);
                    break;
                case LAST_UPDATE_TIME_FIELD:
                    lastUpdateTime = toInstant(parser);
                    break;
                case LOCK_DURATION_SECONDS:
                    lockDurationSeconds = parser.longValue();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        return new GreetJob(name, schedule, isEnabled, enabledTime, disabledTime, lastUpdateTime, lockDurationSeconds);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GreetJob that = (GreetJob) o;
        return Objects.equal(getName(), that.getName())
            && Objects.equal(getSchedule(), that.getSchedule())
            && Objects.equal(isEnabled(), that.isEnabled())
            && Objects.equal(getEnabledTime(), that.getEnabledTime())
            && Objects.equal(getDisabledTime(), that.getDisabledTime())
            && Objects.equal(getLastUpdateTime(), that.getLastUpdateTime())
            && Objects.equal(getLockDurationSeconds(), that.getLockDurationSeconds());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, schedule, isEnabled, enabledTime, lastUpdateTime);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Schedule getSchedule() {
        return schedule;
    }

    @Override
    public boolean isEnabled() {
        return isEnabled;
    }

    @Override
    public Instant getEnabledTime() {
        return enabledTime;
    }

    public Instant getDisabledTime() {
        return disabledTime;
    }

    @Override
    public Instant getLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public Long getLockDurationSeconds() {
        return lockDurationSeconds;
    }

    /**
     * Parse content parser to {@link java.time.Instant}.
     *
     * @param parser json based content parser
     * @return instance of {@link java.time.Instant}
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static Instant toInstant(XContentParser parser) throws IOException {
        if (parser.currentToken() == null || parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return null;
        }
        if (parser.currentToken().isValue()) {
            return Instant.ofEpochMilli(parser.longValue());
        }
        return null;
    }
}
