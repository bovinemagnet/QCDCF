package com.paulsnow.qcdcf.core.exception;

/**
 * Thrown when a table is not suitable for CDC capture.
 * <p>
 * Common causes: missing primary key, unsuitable replica identity,
 * or table not found in the publication.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class InvalidTableException extends CdcException {

    public InvalidTableException(String message) {
        super(message);
    }

    public InvalidTableException(String message, Throwable cause) {
        super(message, cause);
    }
}
