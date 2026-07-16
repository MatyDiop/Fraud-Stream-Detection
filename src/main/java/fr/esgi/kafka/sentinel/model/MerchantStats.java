package fr.esgi.kafka.sentinel.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Accumulateur SEN-5 : stats d'un marchand sur une fenetre de 5 min.
 * volume = nombre de transactions, totalAmount = somme des montants,
 * declinedCount = nombre de transactions DECLINED.
 */
public record MerchantStats(
        @JsonProperty("volume") long volume,
        @JsonProperty("total_amount") double totalAmount,
        @JsonProperty("declined_count") long declinedCount) {

    public static MerchantStats empty() {
        return new MerchantStats(0, 0.0, 0);
    }

    public MerchantStats add(double amount, boolean declined) {
        return new MerchantStats(
                volume + 1,
                totalAmount + amount,
                declinedCount + (declined ? 1 : 0));
    }
}
