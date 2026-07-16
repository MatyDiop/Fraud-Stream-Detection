package fr.esgi.kafka.sentinel;

import fr.esgi.kafka.sentinel.common.Geo;
import fr.esgi.kafka.sentinel.common.JsonSerdes;
import fr.esgi.kafka.sentinel.model.AmountAlert;
import fr.esgi.kafka.sentinel.model.AmountState;
import fr.esgi.kafka.sentinel.model.DlqEntry;
import fr.esgi.kafka.sentinel.model.GeoAlert;
import fr.esgi.kafka.sentinel.model.GeoState;
import fr.esgi.kafka.sentinel.model.Merchant;
import fr.esgi.kafka.sentinel.model.MerchantStats;
import fr.esgi.kafka.sentinel.model.MerchantStatsOut;
import fr.esgi.kafka.sentinel.model.MerchantWindowStats;
import fr.esgi.kafka.sentinel.model.Transaction;
import fr.esgi.kafka.sentinel.model.VelocityAlert;
import fr.esgi.kafka.sentinel.processor.TxDeduplicator;
import fr.esgi.kafka.sentinel.validation.TransactionValidator;
import fr.esgi.kafka.sentinel.validation.ValidationResult;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Construisez ici la topologie du projet SENTINEL.
 * Spring Boot demarre Kafka Streams automatiquement grace a
 * {@code @EnableKafkaStreams} + la config spring.kafka.streams.* de
 * application.yml. Chaque ticket du backlog (README) correspond a un
 * bloc ci-dessous.
 */
@Configuration
@EnableKafkaStreams
public class SentinelTopology {

    /** State store des compteurs d'alertes par type, lu par l'API /alerts/summary. */
    public static final String ALERT_COUNTS_STORE = "sen6-alert-counts";

    @Bean
    public KStream<String, String> pipeline(StreamsBuilder builder) {

        KStream<String, String> rawTx = builder.stream(
                Topics.TRANSACTIONS,
                Consumed.with(Serdes.String(), Serdes.String()));

        // -----------------------------------------------------------------
        // SEN-1 - Ingestion fiable
        //   Parser (JsonNode, jamais Transaction directement : evite les
        //   coercions Jackson silencieuses type "23.44" -> 23.44), valider
        //   champ par champ, router les invalides vers Topics.DLQ avec
        //   message original + raison. Jamais d'exception -> jamais de crash.
        // -----------------------------------------------------------------
        KStream<String, ValidationResult> validated = rawTx
                .mapValues(TransactionValidator::validate);

        Map<String, KStream<String, ValidationResult>> branches = validated
                .split(Named.as("sen1-"))
                .branch((key, result) -> result.isValid(), Branched.as("valid"))
                .defaultBranch(Branched.as("invalid"));

        KStream<String, Transaction> validTx = branches.get("sen1-valid")
                .mapValues(ValidationResult::transaction);

        branches.get("sen1-invalid")
                .mapValues(result -> JsonSerdes.toJson(
                        new DlqEntry(result.reason(), result.rawJson())))
                .to(Topics.DLQ, Produced.with(Serdes.String(), Serdes.String()));

        // -----------------------------------------------------------------
        // SEN-2 - Velocity (>= 5 tx / carte / min)      -> Topics.ALERTS_VELOCITY
        //   Cle deja = card_id -> pas de repartition. Fenetre tumbling 1 min.
        //   Dedoublonnage des tx_id AVANT le comptage (un doublon ne gonfle
        //   pas la velocity). suppress -> une seule alerte finale par fenetre.
        // -----------------------------------------------------------------

        // State store des tx_id deja vus (dedoublonnage), purge > 10 min.
        StoreBuilder<KeyValueStore<String, Long>> seenTxStore =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("sen2-seen-tx"),
                        Serdes.String(), Serdes.Long());
        builder.addStateStore(seenTxStore);

        KStream<String, Transaction> dedupedTx = validTx.processValues(
                () -> new TxDeduplicator("sen2-seen-tx", Duration.ofMinutes(10)),
                "sen2-seen-tx");

        KStream<String, String> velocityAlerts = dedupedTx
                .mapValues(Transaction::txId)
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.ofSizeAndGrace(
                        Duration.ofMinutes(1), Duration.ofMinutes(2)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(
                                "sen2-velocity-counts")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .filter((windowedKey, count) -> count >= 5)
                .map((windowedKey, count) -> {
                    String cardId = windowedKey.key();
                    Instant start = Instant.ofEpochMilli(windowedKey.window().start());
                    Instant end = Instant.ofEpochMilli(windowedKey.window().end());
                    return KeyValue.pair(cardId, JsonSerdes.toJson(
                            VelocityAlert.of(cardId, count, start, end)));
                });
        velocityAlerts.to(Topics.ALERTS_VELOCITY, Produced.with(Serdes.String(), Serdes.String()));


        // -----------------------------------------------------------------
        // SEN-3 - Voyage impossible (> 500 km en < 10 min)   -> Topics.ALERTS_GEO
        //   aggregate qui transporte la derniere position de la carte + une
        //   alerte eventuelle. Haversine (approximation plane refusee).
        //   Condition : distance > 500 km ET dt < 10 min (sur timestamps
        //   embarques). Retardataire (dt < 0) : on ignore, pas de fausse
        //   alerte. Cache desactive pour ne perdre aucun etat porteur d'alerte.
        // -----------------------------------------------------------------
        Serde<Transaction> txSerde = JsonSerdes.of(Transaction.class);
        Serde<GeoState> geoStateSerde = JsonSerdes.of(GeoState.class);
        long tenMinutesMs = TimeUnit.MINUTES.toMillis(10);

        KStream<String, String> geoAlerts = validTx
                .groupByKey(Grouped.with(Serdes.String(), txSerde))
                .aggregate(
                        GeoState::empty,
                        (cardId, tx, state) -> {
                            long ts = Instant.parse(tx.timestamp()).toEpochMilli();
                            if (!state.hasPrevious()) {
                                return new GeoState(tx.lat(), tx.lon(), tx.city(), ts, null);
                            }
                            long dtMs = ts - state.prevTs();
                            if (dtMs < 0) {
                                // retardataire : ne pas comparer ni deplacer la position recente
                                return new GeoState(state.prevLat(), state.prevLon(),
                                        state.prevCity(), state.prevTs(), null);
                            }
                            double dist = Geo.haversineKm(
                                    state.prevLat(), state.prevLon(), tx.lat(), tx.lon());
                            GeoAlert alert = null;
                            if (dist > 500.0 && dtMs < tenMinutesMs) {
                                alert = GeoAlert.of(cardId, state.prevCity(), tx.city(),
                                        dist, dtMs / 60000.0,
                                        Instant.ofEpochMilli(state.prevTs()).toString(),
                                        tx.timestamp());
                            }
                            return new GeoState(tx.lat(), tx.lon(), tx.city(), ts, alert);
                        },
                        Materialized.<String, GeoState, KeyValueStore<Bytes, byte[]>>as(
                                        "sen3-geo-state")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(geoStateSerde)
                                .withCachingDisabled())
                .toStream()
                .filter((cardId, state) -> state != null && state.alert() != null)
                .mapValues(state -> JsonSerdes.toJson(state.alert()));
        geoAlerts.to(Topics.ALERTS_GEO, Produced.with(Serdes.String(), Serdes.String()));

        // -----------------------------------------------------------------
        // SEN-4 - Montant anormal (> 10x la moyenne mobile)  -> Topics.ALERTS_AMOUNT
        //   aggregate {somme, compte} par carte -> moyenne mobile. Piege du
        //   sujet : COMPARER la transaction a la moyenne AVANT de l'incorporer
        //   (sinon un montant 20x gonfle sa propre moyenne et se camoufle).
        //   Ignorer les cartes avec < 5 transactions d'historique.
        // -----------------------------------------------------------------
        Serde<AmountState> amountStateSerde = JsonSerdes.of(AmountState.class);

        KStream<String, String> amountAlerts = validTx
                .groupByKey(Grouped.with(Serdes.String(), txSerde))
                .aggregate(
                        AmountState::empty,
                        (cardId, tx, state) -> {
                            AmountAlert alert = null;
                            if (state.count() >= 5) {
                                double avg = state.sum() / state.count();
                                if (tx.amount() > 10.0 * avg) {
                                    alert = AmountAlert.of(cardId, tx.amount(), avg,
                                            tx.txId(), tx.timestamp());
                                }
                            }
                            // On incorpore la transaction courante APRES l'avoir comparee.
                            return new AmountState(
                                    state.sum() + tx.amount(), state.count() + 1, alert);
                        },
                        Materialized.<String, AmountState, KeyValueStore<Bytes, byte[]>>as(
                                        "sen4-amount-state")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(amountStateSerde)
                                .withCachingDisabled())
                .toStream()
                .filter((cardId, state) -> state != null && state.alert() != null)
                .mapValues(state -> JsonSerdes.toJson(state.alert()));
        amountAlerts.to(Topics.ALERTS_AMOUNT, Produced.with(Serdes.String(), Serdes.String()));

        // -----------------------------------------------------------------
        // SEN-5 - Stats marchands (tumbling 5 min)           -> Topics.MERCHANT_STATS
        //   Repartition : la cle du flux est card_id, on regroupe par
        //   merchant_id -> groupBy cree un topic de repartition (visible dans
        //   topology.describe()). Enrichissement via GlobalKTable (repliquee,
        //   pas de repartition pour la jointure) en leftJoin (un marchand hors
        //   referentiel ne doit pas faire perdre ses stats). suspect si le
        //   taux DECLINED depasse 40 %.
        // -----------------------------------------------------------------
        GlobalKTable<String, Merchant> merchants = builder.globalTable(
                Topics.MERCHANTS,
                Consumed.with(Serdes.String(), JsonSerdes.of(Merchant.class)));

        Serde<MerchantStats> merchantStatsSerde = JsonSerdes.of(MerchantStats.class);

        KStream<String, MerchantStatsOut> merchantStats = validTx
                .groupBy((cardId, tx) -> tx.merchantId(),
                        Grouped.with(Serdes.String(), txSerde))
                .windowedBy(TimeWindows.ofSizeAndGrace(
                        Duration.ofMinutes(5), Duration.ofMinutes(2)))
                .aggregate(
                        MerchantStats::empty,
                        (merchantId, tx, stats) -> stats.add(
                                tx.amount(), "DECLINED".equals(tx.status())),
                        Materialized.<String, MerchantStats, WindowStore<Bytes, byte[]>>as(
                                        "sen5-merchant-stats")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(merchantStatsSerde))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((windowedKey, stats) -> KeyValue.pair(
                        windowedKey.key(),
                        new MerchantWindowStats(
                                windowedKey.key(),
                                stats.volume(),
                                stats.totalAmount(),
                                stats.declinedCount(),
                                Instant.ofEpochMilli(windowedKey.window().start()).toString(),
                                Instant.ofEpochMilli(windowedKey.window().end()).toString())))
                .leftJoin(merchants,
                        (merchantId, stats) -> merchantId,
                        MerchantStatsOut::from);
        merchantStats
                .mapValues(JsonSerdes::toJson)
                .to(Topics.MERCHANT_STATS, Produced.with(Serdes.String(), Serdes.String()));

        // -----------------------------------------------------------------
        // SEN-6 (bonus) - exactly_once_v2 + API REST /alerts/summary
        //   EOS active dans application.yml (processing.guarantee).
        //   Ici : on compte les alertes par type dans un state store
        //   (sen6-alert-counts) interrogeable en Interactive Queries par
        //   AlertsSummaryController (GET /alerts/summary).
        // -----------------------------------------------------------------
        KStream<String, String> merchantOutageTypes = merchantStats
                .filter((merchantId, out) -> out.suspect())
                .map((merchantId, out) -> KeyValue.pair(out.type(), ""));

        velocityAlerts.map((k, v) -> KeyValue.pair("velocity_attack", ""))
                .merge(geoAlerts.map((k, v) -> KeyValue.pair("impossible_travel", "")))
                .merge(amountAlerts.map((k, v) -> KeyValue.pair("big_ticket", "")))
                .merge(merchantOutageTypes)
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(
                                ALERT_COUNTS_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        return rawTx;
    }
}
