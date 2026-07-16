package fr.esgi.kafka.sentinel;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests de topologie (TopologyTestDriver) : la Topology nue construite via
 * StreamsBuilder, sans demarrer Spring ni broker. Chaque test reprend un
 * critere ecrit du sujet.
 */
class SentinelTopologyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Instant T0 = Instant.parse("2026-07-16T10:00:00.000Z");

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> transactions;
    private TestOutputTopic<String, String> dlq;
    private TestOutputTopic<String, String> velocityAlerts;
    private TestOutputTopic<String, String> geoAlerts;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        new SentinelTopology().pipeline(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sentinel-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        driver = new TopologyTestDriver(builder.build(), props);

        transactions = driver.createInputTopic(
                Topics.TRANSACTIONS, new StringSerializer(), new StringSerializer());
        dlq = driver.createOutputTopic(
                Topics.DLQ, new StringDeserializer(), new StringDeserializer());
        velocityAlerts = driver.createOutputTopic(
                Topics.ALERTS_VELOCITY, new StringDeserializer(), new StringDeserializer());
        geoAlerts = driver.createOutputTopic(
                Topics.ALERTS_GEO, new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    /** Fabrique une transaction valide ; les tests alterent ensuite ce dont ils ont besoin. */
    private static String tx(String txId, String cardId, double amount,
                             String city, double lat, double lon, Instant ts) {
        return """
                {"tx_id": "%s", "card_id": "%s", "merchant_id": "mch-001", \
                "merchant_name": "La Cantine 10", "category": "RESTAURANT", \
                "amount": %s, "currency": "EUR", "city": "%s", "lat": %s, "lon": %s, \
                "status": "APPROVED", "timestamp": "%s"}"""
                .formatted(txId, cardId, amount, city, lat, lon, ts);
    }

    // ------------------------------------------------------------------
    // SEN-1
    // ------------------------------------------------------------------

    @Test
    void montantNegatifDoitPartirEnDlq() throws Exception {
        String raw = tx("tx-neg", "card-0001", -12.5, "Paris", 48.8566, 2.3522, T0);
        transactions.pipeInput("card-0001", raw, T0);

        List<String> rejected = dlq.readValuesToList();
        assertEquals(1, rejected.size(), "le montant negatif doit partir en DLQ");
        JsonNode entry = MAPPER.readTree(rejected.get(0));
        assertEquals("amount <= 0", entry.get("reason").asText());
        assertEquals(raw, entry.get("raw").asText(), "la DLQ porte le message original");
    }

    @Test
    void poisonPillNeTuePasLApplicationEtPartEnDlq() throws Exception {
        transactions.pipeInput("card-0002", "{\"tx_id\": \"tx", T0); // JSON tronque

        List<String> rejected = dlq.readValuesToList();
        assertEquals(1, rejected.size());
        assertEquals("JSON illisible", MAPPER.readTree(rejected.get(0)).get("reason").asText());
    }

    @Test
    void transactionValideNEstPasPerdue() {
        transactions.pipeInput("card-0003",
                tx("tx-ok", "card-0003", 23.44, "Paris", 48.8566, 2.3522, T0), T0);
        assertTrue(dlq.isEmpty(), "une transaction valide ne doit pas partir en DLQ");
    }

    // ------------------------------------------------------------------
    // SEN-2
    // ------------------------------------------------------------------

    @Test
    void rafaleDeTransactionsDeclencheVelocity() throws Exception {
        // 5 tx de la meme carte en 40 s, dans la meme fenetre tumbling d'1 min.
        for (int i = 0; i < 5; i++) {
            Instant ts = T0.plusSeconds(10L * i);
            transactions.pipeInput("card-hot",
                    tx("tx-burst-" + i, "card-hot", 20.0, "Paris", 48.8566, 2.3522, ts), ts);
        }
        // Avancer le stream time au-dela de la fenetre + grace (1 + 2 min)
        // pour que suppress(untilWindowCloses) emette le resultat final.
        Instant later = T0.plus(Duration.ofMinutes(5));
        transactions.pipeInput("card-cold",
                tx("tx-later", "card-cold", 10.0, "Paris", 48.8566, 2.3522, later), later);

        List<String> alerts = velocityAlerts.readValuesToList();
        assertEquals(1, alerts.size(), "5 tx dans la minute = exactement une alerte");
        JsonNode alert = MAPPER.readTree(alerts.get(0));
        assertEquals("velocity_attack", alert.get("type").asText());
        assertEquals("card-hot", alert.get("card_id").asText());
        assertEquals(5, alert.get("count").asLong());
    }

    @Test
    void doublonDeTxIdNeGonflePasLaVelocity() {
        // 4 tx distinctes + 1 doublon exact : le compte doit rester a 4 -> silence.
        for (int i = 0; i < 4; i++) {
            Instant ts = T0.plusSeconds(10L * i);
            transactions.pipeInput("card-dup",
                    tx("tx-dup-" + i, "card-dup", 20.0, "Paris", 48.8566, 2.3522, ts), ts);
        }
        Instant tsDup = T0.plusSeconds(45);
        transactions.pipeInput("card-dup",
                tx("tx-dup-0", "card-dup", 20.0, "Paris", 48.8566, 2.3522, tsDup), tsDup);

        Instant later = T0.plus(Duration.ofMinutes(5));
        transactions.pipeInput("card-cold",
                tx("tx-later", "card-cold", 10.0, "Paris", 48.8566, 2.3522, later), later);

        assertTrue(velocityAlerts.isEmpty(),
                "un doublon de tx_id ne doit pas faire passer le compte de 4 a 5");
    }

    // ------------------------------------------------------------------
    // SEN-3
    // ------------------------------------------------------------------

    @Test
    void parisPuisNewYorkEnQuatreMinutesDeclencheImpossibleTravel() throws Exception {
        transactions.pipeInput("card-geo",
                tx("tx-paris", "card-geo", 30.0, "Paris", 48.8566, 2.3522, T0), T0);
        Instant t1 = T0.plus(Duration.ofMinutes(4));
        transactions.pipeInput("card-geo",
                tx("tx-nyc", "card-geo", 200.0, "New York", 40.7128, -74.006, t1), t1);

        List<String> alerts = geoAlerts.readValuesToList();
        assertEquals(1, alerts.size());
        JsonNode alert = MAPPER.readTree(alerts.get(0));
        assertEquals("impossible_travel", alert.get("type").asText());
        assertEquals("Paris", alert.get("from_city").asText());
        assertEquals("New York", alert.get("to_city").asText());
        assertTrue(alert.get("distance_km").asDouble() > 5800,
                "Paris-NYC ~5837 km par haversine");
    }

    @Test
    void parisPuisBruxellesEnDeuxHeuresNeDeclencheRien() {
        transactions.pipeInput("card-slow",
                tx("tx-paris", "card-slow", 30.0, "Paris", 48.8566, 2.3522, T0), T0);
        Instant t1 = T0.plus(Duration.ofHours(2));
        transactions.pipeInput("card-slow",
                tx("tx-bxl", "card-slow", 25.0, "Bruxelles", 50.8503, 4.3517, t1), t1);

        assertTrue(geoAlerts.isEmpty(),
                "260 km en 2 h : ni la distance ni le delai ne matchent");
    }

    @Test
    void retardataireNeDeclenchePasDeFausseAlerteGeo() {
        // Position courante : Paris a T0+30min.
        Instant now = T0.plus(Duration.ofMinutes(30));
        transactions.pipeInput("card-late",
                tx("tx-now", "card-late", 30.0, "Paris", 48.8566, 2.3522, now), now);
        // Arrive ENSUITE un retardataire horodate 30 min AVANT, depuis New York.
        transactions.pipeInput("card-late",
                tx("tx-old", "card-late", 40.0, "New York", 40.7128, -74.006, T0), now);

        assertTrue(geoAlerts.isEmpty(),
                "un evenement plus ancien que la derniere position ne doit pas alerter");
    }
}
