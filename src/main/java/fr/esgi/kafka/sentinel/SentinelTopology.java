package fr.esgi.kafka.sentinel;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

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

    private static final Logger LOG = LoggerFactory.getLogger(SentinelTopology.class);

    @Bean
    public KStream<String, String> pipeline(StreamsBuilder builder) {

        KStream<String, String> rawTx = builder.stream(
                Topics.TRANSACTIONS,
                Consumed.with(Serdes.String(), Serdes.String()));

        // Sanity check de demarrage : verifiez la connexion au cluster,
        // puis SUPPRIMEZ ce peek (il pollue les logs et coute cher).
        rawTx.peek((key, value) -> LOG.info("[sentinel] {} -> {}", key, value));

        // -----------------------------------------------------------------
        // SEN-1 - Ingestion fiable
        //   Parser (model.Transaction), valider (champs requis, types,
        //   amount > 0, currency dans {EUR, USD, GBP}, status dans
        //   {APPROVED, DECLINED}, lat/lon plausibles, timestamp ISO-8601),
        //   router les invalides vers Topics.DLQ avec message original
        //   + raison.
        //   Pistes : split()/branch(), JsonSerdes.parseOrNull(...).
        // -----------------------------------------------------------------

        // SEN-2 - Velocity (>= 5 tx / carte / min)      -> Topics.ALERTS_VELOCITY
        // SEN-3 - Voyage impossible (> 500 km en < 10 min)
        //                                               -> Topics.ALERTS_GEO
        // SEN-4 - Montant anormal (> 10x la moyenne mobile de la carte)
        //                                               -> Topics.ALERTS_AMOUNT
        // SEN-5 - Stats marchands (tumbling 5 min, join sentinel.merchants,
        //         flag si taux DECLINED > 40 %)         -> Topics.MERCHANT_STATS
        // SEN-6 (bonus) - exactly_once_v2 + API REST /alerts/summary (cf. README)

        return rawTx;
    }
}
