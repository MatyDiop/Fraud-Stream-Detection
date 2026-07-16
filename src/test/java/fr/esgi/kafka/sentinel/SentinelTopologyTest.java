package fr.esgi.kafka.sentinel;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Squelette de tests unitaires de topologie (bonus "tests").
 * Outil : TopologyTestDriver (deja en scope test). On teste la Topology
 * nue construite via StreamsBuilder, sans demarrer Spring ni broker.
 */
class SentinelTopologyTest {

    @Test
    @Disabled("A implementer : un montant negatif doit finir dans la DLQ")
    void montantNegatifDoitPartirEnDlq() {
    }

    @Test
    @Disabled("A implementer : 5 tx en 1 min sur la meme carte = alerte velocity")
    void rafaleDeTransactionsDeclencheVelocity() {
    }
}
