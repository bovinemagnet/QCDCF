package com.paulsnow.qcdcf.core.sink;

/**
 * Result of publishing one or more events to a sink.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public sealed interface PublishResult {

    /** Publication succeeded. */
    record Success(int eventCount) implements PublishResult {}

    /** Publication failed. */
    record Failure(String reason, Throwable cause) implements PublishResult {
        public Failure(String reason) {
            this(reason, null);
        }
    }

    /** Returns {@code true} if this result represents a successful publication. */
    default boolean isSuccess() {
        return this instanceof Success;
    }
}
