package fr.esgi.kafka.sentinel.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Fiche marchand (topic compacte sentinel.merchants, cle = merchant_id). */
public record Merchant(
        @JsonProperty("merchant_id") String merchantId,
        @JsonProperty("name") String name,
        @JsonProperty("category") String category,
        @JsonProperty("city") String city) {
}
