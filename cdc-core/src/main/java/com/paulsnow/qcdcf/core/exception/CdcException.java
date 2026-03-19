package com.paulsnow.qcdcf.core.exception;

/**
 * Base exception for all CDC framework errors.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class CdcException extends RuntimeException {

    public CdcException(String message) {
        super(message);
    }

    public CdcException(String message, Throwable cause) {
        super(message, cause);
    }
}
