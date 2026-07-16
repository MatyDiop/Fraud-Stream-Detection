package fr.esgi.kafka.sentinel.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Accumulateur SEN-4 : somme et nombre des montants d'une carte (pour la
 * moyenne mobile) + l'alerte eventuellement declenchee par la transaction
 * courante. La moyenne = sum / count sur les transactions PRECEDENTES.
 */
public record AmountState(
        @JsonProperty("sum") double sum,
        @JsonProperty("count") long count,
        @JsonProperty("alert") AmountAlert alert) {

    /** Etat initial, avant toute transaction. */
    public static AmountState empty() {
        return new AmountState(0.0, 0, null);
    }
}
