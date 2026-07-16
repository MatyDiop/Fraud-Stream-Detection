package fr.esgi.kafka.sentinel.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/** Alerte SEN-2 : une carte a fait trop de transactions dans une fenetre. */
public record VelocityAlert(
        @JsonProperty("type") String type,
        @JsonProperty("card_id") String cardId,
        @JsonProperty("count") long count,
        @JsonProperty("window_start") String windowStart,
        @JsonProperty("window_end") String windowEnd) {

    public static VelocityAlert of(String cardId, long count, Instant start, Instant end) {
        return new VelocityAlert("velocity_attack", cardId, count,
                start.toString(), end.toString());
    }
}
