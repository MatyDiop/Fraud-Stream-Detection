package fr.esgi.kafka.sentinel.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import fr.esgi.kafka.sentinel.model.Transaction;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Set;

/**
 * Valide un message brut de sentinel.transactions champ par champ.
 * Ne leve jamais d'exception : tout echec renvoie un ValidationResult
 * invalide avec une raison precise (le correcteur compte les raisons
 * distinctes dans la DLQ).
 */
public final class TransactionValidator {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Set<String> CURRENCIES = Set.of("EUR", "USD", "GBP");
    private static final Set<String> STATUSES = Set.of("APPROVED", "DECLINED");
    private static final String[] REQUIRED_FIELDS = {
            "tx_id", "card_id", "merchant_id", "merchant_name", "category",
            "amount", "currency", "city", "lat", "lon", "status", "timestamp"
    };

    private TransactionValidator() {
    }

    /**
     * @param key cle Kafka du record (attendue = card_id ; toute la pipeline
     *            aval - SEN-2/3/4 - regroupe par cle sans repartition sur
     *            cette hypothese. Une divergence serait un bug silencieux :
     *            les agregats par carte seraient calcules sur le mauvais
     *            regroupement, sans aucune exception pour le signaler).
     */
    public static ValidationResult validate(String key, String raw) {
        JsonNode node;
        try {
            node = MAPPER.readTree(raw);
        } catch (Exception e) {
            return ValidationResult.invalid(raw, "JSON illisible");
        }
        if (node == null || node.isNull() || !node.isObject()) {
            return ValidationResult.invalid(raw, "JSON illisible");
        }

        for (String field : REQUIRED_FIELDS) {
            JsonNode value = node.get(field);
            if (value == null || value.isNull()) {
                return ValidationResult.invalid(raw, "champ requis manquant: " + field);
            }
        }

        String cardId = node.get("card_id").asText();
        if (key == null) {
            return ValidationResult.invalid(raw, "cle Kafka absente (attendu card_id)");
        }
        if (!key.equals(cardId)) {
            return ValidationResult.invalid(raw, "cle Kafka != card_id");
        }

        JsonNode amountNode = node.get("amount");
        if (!amountNode.isNumber()) {
            return ValidationResult.invalid(raw, "amount: type invalide");
        }
        double amount = amountNode.asDouble();
        if (amount <= 0) {
            return ValidationResult.invalid(raw, "amount <= 0");
        }

        String currency = node.get("currency").asText();
        if (!CURRENCIES.contains(currency)) {
            return ValidationResult.invalid(raw, "currency inconnue: " + currency);
        }

        String status = node.get("status").asText();
        if (!STATUSES.contains(status)) {
            return ValidationResult.invalid(raw, "status inconnu: " + status);
        }

        JsonNode latNode = node.get("lat");
        JsonNode lonNode = node.get("lon");
        if (!latNode.isNumber() || !lonNode.isNumber()) {
            return ValidationResult.invalid(raw, "lat/lon: type invalide");
        }
        double lat = latNode.asDouble();
        double lon = lonNode.asDouble();
        if (lat < -90 || lat > 90) {
            return ValidationResult.invalid(raw, "lat hors bornes");
        }
        if (lon < -180 || lon > 180) {
            return ValidationResult.invalid(raw, "lon hors bornes");
        }

        String timestamp = node.get("timestamp").asText();
        try {
            Instant.parse(timestamp);
        } catch (DateTimeParseException e) {
            return ValidationResult.invalid(raw, "timestamp non ISO-8601");
        }

        Transaction transaction = new Transaction(
                node.get("tx_id").asText(),
                cardId,
                node.get("merchant_id").asText(),
                node.get("merchant_name").asText(),
                node.get("category").asText(),
                amount,
                currency,
                node.get("city").asText(),
                lat,
                lon,
                status,
                timestamp);
        return ValidationResult.valid(transaction);
    }
}








/*c'est le fichier qui valide les transactions, si ces transaction passent tous les filtres elle est valide et
 * on continue la pipeline, sinon elle est invalide et on la met dans la DLQ avec la raison de l'invalidité 
*/