package fr.esgi.kafka.sentinel.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Format des messages de la DLQ : le brut recu + la raison lisible du rejet. */
public record DlqEntry(
        @JsonProperty("reason") String reason,
        @JsonProperty("raw") String raw) {
}
