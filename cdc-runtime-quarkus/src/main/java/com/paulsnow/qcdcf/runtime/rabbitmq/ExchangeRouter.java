package com.paulsnow.qcdcf.runtime.rabbitmq;

import com.paulsnow.qcdcf.model.ChangeEnvelope;

/**
 * Routes change events to a RabbitMQ exchange with a table-based routing key.
 * <p>
 * Routing key pattern: {@code {schema}.{table}} (e.g., {@code public.customer}).
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class ExchangeRouter {
    private final String exchangeName;

    public ExchangeRouter(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    public String exchangeName() {
        return exchangeName;
    }

    public String routingKey(ChangeEnvelope event) {
        return event.tableId().canonicalName();
    }
}
