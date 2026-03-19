package com.paulsnow.qcdcf.testkit.assertions;

import com.paulsnow.qcdcf.model.CaptureMode;
import com.paulsnow.qcdcf.model.ChangeEnvelope;
import com.paulsnow.qcdcf.model.OperationType;
import com.paulsnow.qcdcf.model.TableId;
import org.assertj.core.api.AbstractAssert;

/**
 * AssertJ-style assertions for {@link ChangeEnvelope}.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class ChangeEnvelopeAssertions extends AbstractAssert<ChangeEnvelopeAssertions, ChangeEnvelope> {

    private ChangeEnvelopeAssertions(ChangeEnvelope actual) {
        super(actual, ChangeEnvelopeAssertions.class);
    }

    public static ChangeEnvelopeAssertions assertThatEnvelope(ChangeEnvelope actual) {
        return new ChangeEnvelopeAssertions(actual);
    }

    public ChangeEnvelopeAssertions hasOperation(OperationType expected) {
        isNotNull();
        if (actual.operation() != expected) {
            failWithMessage("Expected operation <%s> but was <%s>", expected, actual.operation());
        }
        return this;
    }

    public ChangeEnvelopeAssertions hasCaptureMode(CaptureMode expected) {
        isNotNull();
        if (actual.captureMode() != expected) {
            failWithMessage("Expected capture mode <%s> but was <%s>", expected, actual.captureMode());
        }
        return this;
    }

    public ChangeEnvelopeAssertions hasTable(TableId expected) {
        isNotNull();
        if (!actual.tableId().equals(expected)) {
            failWithMessage("Expected table <%s> but was <%s>", expected, actual.tableId());
        }
        return this;
    }

    public ChangeEnvelopeAssertions hasKeyValue(String column, Object value) {
        isNotNull();
        Object actualValue = actual.key().columns().get(column);
        if (!value.equals(actualValue)) {
            failWithMessage("Expected key column <%s> to be <%s> but was <%s>", column, value, actualValue);
        }
        return this;
    }

    public ChangeEnvelopeAssertions hasConnectorId(String expected) {
        isNotNull();
        if (!actual.connectorId().equals(expected)) {
            failWithMessage("Expected connector ID <%s> but was <%s>", expected, actual.connectorId());
        }
        return this;
    }
}
