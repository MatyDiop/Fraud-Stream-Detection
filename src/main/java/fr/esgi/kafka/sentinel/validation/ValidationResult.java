package fr.esgi.kafka.sentinel.validation;

import fr.esgi.kafka.sentinel.model.Transaction;

/**
 * Resultat d'une validation : soit une Transaction exploitable, soit le
 * message brut accompagne de la raison du rejet. Jamais d'exception : le
 * pipeline route sur isValid(), pas sur un catch.
 */
public record ValidationResult(Transaction transaction, String rawJson, String reason) {

    public static ValidationResult valid(Transaction transaction) {
        return new ValidationResult(transaction, null, null);
    }

    public static ValidationResult invalid(String rawJson, String reason) {
        return new ValidationResult(null, rawJson, reason);
    }

    public boolean isValid() {
        return reason == null;
    }
}
