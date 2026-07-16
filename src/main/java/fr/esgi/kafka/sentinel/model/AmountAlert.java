package fr.esgi.kafka.sentinel.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Alerte SEN-4 : un montant anormalement eleve par rapport a la moyenne de la carte. */
public record AmountAlert(
        @JsonProperty("type") String type,
        @JsonProperty("card_id") String cardId,
        @JsonProperty("amount") double amount,
        @JsonProperty("average") double average,
        @JsonProperty("ratio") double ratio,
        @JsonProperty("tx_id") String txId,
        @JsonProperty("timestamp") String timestamp) {

    public static AmountAlert of(String cardId, double amount, double average,
                                 String txId, String timestamp) {
        return new AmountAlert("big_ticket", cardId,
                amount,
                Math.round(average * 100) / 100.0,
                Math.round(amount / average * 10) / 10.0,
                txId, timestamp);
    }
}
