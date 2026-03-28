package com.paulsnow.qcdcf.core.exception;

/**
 * Thrown when checkpoint persistence fails.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class CheckpointException extends CdcException {

    public CheckpointException(String message) {
        super(message);
    }

    public CheckpointException(String message, Throwable cause) {
        super(message, cause);
    }
}
