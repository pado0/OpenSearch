/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackers.TaskResourceUsageTracker;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackerType.CPU_USAGE_TRACKER;

/**
 * CpuUsageTracker evaluates if the task has consumed too many CPU cycles than allowed.
 *
 * @opensearch.internal
 */
public class CpuUsageTracker extends TaskResourceUsageTracker {

    private final LongSupplier thresholdSupplier;

    public CpuUsageTracker(LongSupplier thresholdSupplier) {
        this(thresholdSupplier, (task) -> {
            long usage = task.getTotalResourceStats().getCpuTimeInNanos();
            long threshold = thresholdSupplier.getAsLong();

            if (usage < threshold) {
                return Optional.empty();
            }

            return Optional.of(
                new TaskCancellation.Reason(
                    "cpu usage exceeded ["
                        + new TimeValue(usage, TimeUnit.NANOSECONDS)
                        + " >= "
                        + new TimeValue(threshold, TimeUnit.NANOSECONDS)
                        + "]",
                    1  // TODO: fine-tune the cancellation score/weight
                )
            );
        });
    }

    public CpuUsageTracker(LongSupplier thresholdSupplier, ResourceUsageBreachEvaluator resourceUsageBreachEvaluator) {
        this.thresholdSupplier = thresholdSupplier;
        this.resourceUsageBreachEvaluator = resourceUsageBreachEvaluator;
    }

    @Override
    public String name() {
        return CPU_USAGE_TRACKER.getName();
    }

    @Override
    public TaskResourceUsageTracker.Stats stats(List<? extends Task> activeTasks) {
        long currentMax = activeTasks.stream().mapToLong(t -> t.getTotalResourceStats().getCpuTimeInNanos()).max().orElse(0);
        long currentAvg = (long) activeTasks.stream().mapToLong(t -> t.getTotalResourceStats().getCpuTimeInNanos()).average().orElse(0);
        return new Stats(getCancellations(), currentMax, currentAvg);
    }

    /**
     * Stats related to CpuUsageTracker.
     */
    public static class Stats implements TaskResourceUsageTracker.Stats {
        private final long cancellationCount;
        private final long currentMax;
        private final long currentAvg;

        /**
         * Private constructor that takes a builder.
         * This is the sole entry point for creating a new Stats object.
         * @param builder The builder instance containing all the values.
         */
        private Stats(Builder builder){
            this.cancellationCount = builder.cancellationCount;
            this.currentMax = builder.currentMax;
            this.currentAvg = builder.currentAvg;
        }

        /**
         * This constructor will be deprecated in 4.0
         * Use Builder to create Stats object
         */
        @Deprecated
        public Stats(long cancellationCount, long currentMax, long currentAvg) {
            this.cancellationCount = cancellationCount;
            this.currentMax = currentMax;
            this.currentAvg = currentAvg;
        }

        public Stats(StreamInput in) throws IOException {
            this(in.readVLong(), in.readVLong(), in.readVLong());
        }

        /**DirectoryFileTransferTracker
         * Builder for the {@link Stats} class.
         * Provides a fluent API for constructing a Stats object.
         */
        public static class Builder {
            private long cancellationCount = 0;
            private long currentMax = 0;
            private long currentAvg = 0;

            public Builder() {}

            public Builder cancellationCount(long count) {
                this.cancellationCount = count;
                return this;
            }

            public Builder currentMax(long max) {
                this.currentMax = max;
                return this;
            }

            public Builder currentAvg(long avg) {
                this.currentAvg = avg;
                return this;
            }

            /**
             * Creates a {@link Stats} object from the builder's current state.
             * @return A new Stats instance.
             */
            public Stats build() {
                return new Stats(this);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .field("cancellation_count", cancellationCount)
                .humanReadableField("current_max_millis", "current_max", new TimeValue(currentMax, TimeUnit.NANOSECONDS))
                .humanReadableField("current_avg_millis", "current_avg", new TimeValue(currentAvg, TimeUnit.NANOSECONDS))
                .endObject();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(cancellationCount);
            out.writeVLong(currentMax);
            out.writeVLong(currentAvg);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Stats stats = (Stats) o;
            return cancellationCount == stats.cancellationCount && currentMax == stats.currentMax && currentAvg == stats.currentAvg;
        }

        @Override
        public int hashCode() {
            return Objects.hash(cancellationCount, currentMax, currentAvg);
        }
    }
}
