package com.paulsnow.qcdcf.core.sink;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThatCode;

class EventSinkCloseTest {

    @Test
    void defaultCloseIsNoOp() throws Exception {
        var sink = new InMemoryEventSink();
        assertThatCode(sink::close).doesNotThrowAnyException();
    }
}
