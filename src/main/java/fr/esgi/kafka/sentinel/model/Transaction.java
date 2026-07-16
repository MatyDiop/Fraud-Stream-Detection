package fr.esgi.kafka.sentinel.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Transaction carte (topic sentinel.transactions, cle = card_id).
 * currency : EUR | USD | GBP. status : APPROVED | DECLINED.
 */
public record Transaction(
        @JsonProperty("tx_id") String txId,
        @JsonProperty("card_id") String cardId,
        @JsonProperty("merchant_id") String merchantId,
        @JsonProperty("merchant_name") String merchantName,
        @JsonProperty("category") String category,
        @JsonProperty("amount") Double amount,
        @JsonProperty("currency") String currency,
        @JsonProperty("city") String city,
        @JsonProperty("lat") Double lat,
        @JsonProperty("lon") Double lon,
        @JsonProperty("status") String status,
        @JsonProperty("timestamp") String timestamp) {
}
