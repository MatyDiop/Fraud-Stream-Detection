package fr.esgi.kafka.sentinel;

/**
 * Noms des topics du projet.
 * Entrees fournies par l'enseignant (lecture seule) ; sorties prefixees
 * par votre code groupe (variable d'environnement GROUPE, ex: grp07).
 */
public final class Topics {

    private Topics() {
    }

    // --- Entrees (fournies, ne pas ecrire dedans) ---
    public static final String TRANSACTIONS = "sentinel.transactions";
    public static final String MERCHANTS = "sentinel.merchants";

    /** Code groupe, obligatoire sur le cluster partage. */
    public static final String GROUPE =
            System.getenv().getOrDefault("GROUPE", "grp00");

    public static String out(String suffix) {
        return GROUPE + "." + suffix;
    }

    // --- Sorties (a produire par votre application) ---
    public static final String DLQ = out("sentinel.dlq");
    public static final String ALERTS_VELOCITY = out("sentinel.alerts.velocity");
    public static final String ALERTS_GEO = out("sentinel.alerts.geo");
    public static final String ALERTS_AMOUNT = out("sentinel.alerts.amount");
    public static final String MERCHANT_STATS = out("sentinel.merchant.stats");
}
