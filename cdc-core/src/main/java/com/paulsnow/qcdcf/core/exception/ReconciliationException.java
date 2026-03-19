package com.paulsnow.qcdcf.core.exception;

/**
 * Thrown when watermark reconciliation encounters an unrecoverable error.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class ReconciliationException extends CdcException {

    public ReconciliationException(String message) {
        super(message);
    }

    public ReconciliationException(String message, Throwable cause) {
        super(message, cause);
    }
}
