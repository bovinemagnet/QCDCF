package com.paulsnow.qcdcf.core.exception;

/**
 * Thrown when event publication to a sink fails irrecoverably.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class SinkPublishException extends CdcException {

    public SinkPublishException(String message) {
        super(message);
    }

    public SinkPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
