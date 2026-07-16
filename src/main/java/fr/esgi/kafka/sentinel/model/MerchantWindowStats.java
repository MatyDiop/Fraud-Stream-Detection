package fr.esgi.kafka.sentinel.model;

/**
 * Stats d'un marchand pour une fenetre donnee, portant le merchant_id et les
 * bornes de fenetre. Sert d'entree a la jointure GlobalKTable (transport
 * interne, non serialise vers un topic).
 */
public record MerchantWindowStats(
        String merchantId,
        long volume,
        double totalAmount,
        long declinedCount,
        String windowStart,
        String windowEnd) {
}
