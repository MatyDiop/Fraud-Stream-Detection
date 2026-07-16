package fr.esgi.kafka.sentinel.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Alerte SEN-3 : deux paiements d'une meme carte trop eloignes en trop peu de temps. */
public record GeoAlert(
        @JsonProperty("type") String type,
        @JsonProperty("card_id") String cardId,
        @JsonProperty("from_city") String fromCity,
        @JsonProperty("to_city") String toCity,
        @JsonProperty("distance_km") double distanceKm,
        @JsonProperty("minutes") double minutes,
        @JsonProperty("from_ts") String fromTs,
        @JsonProperty("to_ts") String toTs) {

    public static GeoAlert of(String cardId, String fromCity, String toCity,
                              double distanceKm, double minutes,
                              String fromTs, String toTs) {
        return new GeoAlert("impossible_travel", cardId, fromCity, toCity,
                Math.round(distanceKm * 10) / 10.0,
                Math.round(minutes * 10) / 10.0,
                fromTs, toTs);
    }
}
