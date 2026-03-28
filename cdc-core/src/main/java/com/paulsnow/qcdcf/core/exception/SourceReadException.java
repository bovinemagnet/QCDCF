package com.paulsnow.qcdcf.core.exception;

/**
 * Thrown when reading from the source (WAL or snapshot) fails.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class SourceReadException extends CdcException {

    public SourceReadException(String message) {
        super(message);
    }

    public SourceReadException(String message, Throwable cause) {
        super(message, cause);
    }
}
