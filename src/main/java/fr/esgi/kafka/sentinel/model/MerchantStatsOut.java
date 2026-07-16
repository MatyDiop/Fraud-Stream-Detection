package fr.esgi.kafka.sentinel.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Sortie SEN-5 : stats marchand enrichies + flag suspect (taux DECLINED > 40 %). */
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

    public static MerchantStatsOut from(MerchantWindowStats s, Merchant m) {
        double rate = s.volume() == 0 ? 0.0 : (double) s.declinedCount() / s.volume();
        boolean suspect = rate > 0.40;
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
