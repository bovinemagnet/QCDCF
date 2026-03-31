package com.paulsnow.qcdcf.runtime.rabbitmq;

import com.paulsnow.qcdcf.core.sink.PublishResult;
import com.paulsnow.qcdcf.model.*;
import com.paulsnow.qcdcf.runtime.kafka.EventSerializer;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RabbitMQEventSinkTest {
    private Channel channel;
    private RabbitMQEventSink sink;

    @BeforeEach
    void setUp() {
        channel = mock(Channel.class);
        sink = new RabbitMQEventSink(channel, new ExchangeRouter("qcdcf"), new EventSerializer(), "topic");
    }

    @Test
    void publishSendsToCorrectExchangeAndRoutingKey() throws Exception {
        PublishResult result = sink.publish(sampleEvent("public", "customer"));
        assertThat(result.isSuccess()).isTrue();
        verify(channel).basicPublish(eq("qcdcf"), eq("public.customer"), any(AMQP.BasicProperties.class), any(byte[].class));
    }

    @Test
    void publishReturnsFailureOnIOException() throws Exception {
        doThrow(new IOException("connection lost")).when(channel)
                .basicPublish(anyString(), anyString(), any(), any(byte[].class));
        PublishResult result = sink.publish(sampleEvent("public", "customer"));
        assertThat(result.isSuccess()).isFalse();
    }

    @Test
    void batchFailsOnFirstError() throws Exception {
        doNothing().doThrow(new IOException("connection lost"))
                .when(channel).basicPublish(anyString(), anyString(), any(), any(byte[].class));
        PublishResult result = sink.publishBatch(List.of(
                sampleEvent("public", "customer"), sampleEvent("public", "orders")));
        assertThat(result.isSuccess()).isFalse();
    }

    @Test
    void closeClosesChannel() throws Exception {
        when(channel.isOpen()).thenReturn(true);
        when(channel.getConnection()).thenReturn(mock(com.rabbitmq.client.Connection.class));
        sink.close();
        verify(channel).close();
    }

    private ChangeEnvelope sampleEvent(String schema, String table) {
        return new ChangeEnvelope(UUID.randomUUID(), "test", new TableId(schema, table),
                OperationType.INSERT, CaptureMode.LOG, new RowKey(Map.of("id", 1)),
                null, Map.of("id", 1), new SourcePosition(1000L, Instant.now()),
                Instant.now(), null, Map.of());
    }
}
