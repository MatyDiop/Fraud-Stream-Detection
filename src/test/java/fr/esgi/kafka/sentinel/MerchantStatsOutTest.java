package fr.esgi.kafka.sentinel;

import fr.esgi.kafka.sentinel.model.Merchant;
import fr.esgi.kafka.sentinel.model.MerchantStatsOut;
import fr.esgi.kafka.sentinel.model.MerchantWindowStats;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SEN-5 : construction de la sortie enrichie et règle du flag suspect.
 * Le suspect exige taux DECLINED > 40 % ET un volume significatif
 * (MIN_VOLUME_SUSPECT) — amélioration contre les faux positifs à faible volume.
 */
class MerchantStatsOutTest {

    private static MerchantWindowStats stats(long volume, long declined) {
        return new MerchantWindowStats("mch-001", volume, 100.0, declined,
                "2026-07-16T10:00:00Z", "2026-07-16T10:05:00Z");
    }

    @Test
    void marchandEnPanneAvecVolumeSuffisantEstSuspect() {
        // 10 tx, 7 refusées = 70 % sur un volume significatif -> merchant_outage
        MerchantStatsOut out = MerchantStatsOut.from(stats(10, 7), null);
        assertTrue(out.suspect());
        assertEquals("merchant_outage", out.type());
        assertEquals(0.7, out.declinedRate());
    }

    @Test
    void faibleVolumeNEstPasSuspectMemeAvecTauxEleve() {
        // 2 tx, 1 refusée = 50 % (> 40 %) MAIS volume < MIN_VOLUME_SUSPECT
        // -> ne doit PAS être flaggé (c'est l'amélioration).
        MerchantStatsOut out = MerchantStatsOut.from(stats(2, 1), null);
        assertFalse(out.suspect(), "un marchand à 2 tx ne doit pas être jugé en panne");
        assertEquals("merchant_stats", out.type());
    }

    @Test
    void tauxSousLeSeuilNEstJamaisSuspect() {
        // 20 tx, 5 refusées = 25 % < 40 % -> régime normal
        MerchantStatsOut out = MerchantStatsOut.from(stats(20, 5), null);
        assertFalse(out.suspect());
    }

    @Test
    void enrichissementViaReferentielMarchand() {
        Merchant m = new Merchant("mch-001", "Terminal Louche", "VTC", "Lille");
        MerchantStatsOut out = MerchantStatsOut.from(stats(10, 7), m);
        assertEquals("Terminal Louche", out.name());
        assertEquals("Lille", out.city());
    }

    @Test
    void marchandHorsReferentielGardeSesStats() {
        // leftJoin : merchant null -> name/city null mais stats conservées
        MerchantStatsOut out = MerchantStatsOut.from(stats(10, 7), null);
        assertEquals(null, out.name());
        assertEquals(10, out.volume());
    }
}
