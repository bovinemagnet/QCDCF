package com.paulsnow.qcdcf.runtime.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.paulsnow.qcdcf.model.ChangeEnvelope;

/**
 * Serialises {@link ChangeEnvelope} to JSON bytes for Kafka publication.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class EventSerializer {

    private final ObjectMapper objectMapper;

    public EventSerializer() {
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /**
     * Serialises the given event to a JSON byte array.
     *
     * @param event the change event to serialise
     * @return JSON bytes
     * @throws IllegalStateException if serialisation fails
     */
    public byte[] serialise(ChangeEnvelope event) {
        try {
            return objectMapper.writeValueAsBytes(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialise ChangeEnvelope", e);
        }
    }
}
