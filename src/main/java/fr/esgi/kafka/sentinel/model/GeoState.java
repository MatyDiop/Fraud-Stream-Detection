package fr.esgi.kafka.sentinel.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Accumulateur SEN-3 : la derniere position connue d'une carte + l'alerte
 * eventuellement declenchee par la transaction courante. C'est ce qui est
 * memorise dans le state store entre deux transactions de la meme carte.
 */
public record GeoState(
        @JsonProperty("prev_lat") Double prevLat,
        @JsonProperty("prev_lon") Double prevLon,
        @JsonProperty("prev_city") String prevCity,
        @JsonProperty("prev_ts") Long prevTs,
        @JsonProperty("alert") GeoAlert alert) {

    /** Etat initial, avant toute transaction. */
    public static GeoState empty() {
        return new GeoState(null, null, null, null, null);
    }

    public boolean hasPrevious() {
        return prevTs != null;
    }
}
