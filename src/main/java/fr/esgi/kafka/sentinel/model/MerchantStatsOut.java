package fr.esgi.kafka.sentinel.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Sortie SEN-5 : stats marchand enrichies + flag suspect.
 *
 * <p>Un marchand est {@code suspect} si son taux de DECLINED dépasse 40 %
 * <b>ET</b> qu'il a traité au moins {@link #MIN_VOLUME_SUSPECT} transactions
 * sur la fenêtre. Ce plancher de volume évite de crier « panne » sur un
 * marchand à 2 transactions dont 1 refusée (50 % mais statistiquement
 * insignifiant) — même logique que le « ≥ 5 transactions » de SEN-4.
 */
public record MerchantStatsOut(
        @JsonProperty("type") String type,
        @JsonProperty("merchant_id") String merchantId,
        @JsonProperty("name") String name,
        @JsonProperty("category") String category,
        @JsonProperty("city") String city,
        @JsonProperty("volume") long volume,
        @JsonProperty("total_amount") double totalAmount,
        @JsonProperty("declined_rate") double declinedRate,
        @JsonProperty("suspect") boolean suspect,
        @JsonProperty("window_start") String windowStart,
        @JsonProperty("window_end") String windowEnd) {

    /** Volume minimum pour qu'un fort taux de DECLINED soit jugé significatif. */
    public static final long MIN_VOLUME_SUSPECT = 5;

    public static MerchantStatsOut from(MerchantWindowStats s, Merchant m) {
        double rate = s.volume() == 0 ? 0.0 : (double) s.declinedCount() / s.volume();
        boolean suspect = rate > 0.40 && s.volume() >= MIN_VOLUME_SUSPECT;
        return new MerchantStatsOut(
                suspect ? "merchant_outage" : "merchant_stats",
                s.merchantId(),
                m != null ? m.name() : null,
                m != null ? m.category() : null,
                m != null ? m.city() : null,
                s.volume(),
                Math.round(s.totalAmount() * 100) / 100.0,
                Math.round(rate * 1000) / 1000.0,
                suspect,
                s.windowStart(),
                s.windowEnd());
    }
}
