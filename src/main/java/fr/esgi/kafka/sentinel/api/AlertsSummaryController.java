package fr.esgi.kafka.sentinel.api;

import fr.esgi.kafka.sentinel.SentinelTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * SEN-6 - API REST de supervision.
 * GET /alerts/summary renvoie les compteurs d'alertes par type, lus en direct
 * dans le state store {@code sen6-alert-counts} via Interactive Queries.
 */
@RestController
public class AlertsSummaryController {

    private final StreamsBuilderFactoryBean factoryBean;

    public AlertsSummaryController(StreamsBuilderFactoryBean factoryBean) {
        this.factoryBean = factoryBean;
    }

    @GetMapping("/alerts/summary")
    public ResponseEntity<Map<String, Long>> summary() {
        KafkaStreams streams = factoryBean.getKafkaStreams();
        if (streams == null || streams.state() != KafkaStreams.State.RUNNING) {
            // Le pipeline n'est pas (encore) pret a etre interroge.
            return ResponseEntity.status(503).build();
        }

        ReadOnlyKeyValueStore<String, Long> store = streams.store(
                StoreQueryParameters.fromNameAndType(
                        SentinelTopology.ALERT_COUNTS_STORE,
                        QueryableStoreTypes.keyValueStore()));

        Map<String, Long> counts = new LinkedHashMap<>();
        try (KeyValueIterator<String, Long> it = store.all()) {
            while (it.hasNext()) {
                KeyValue<String, Long> entry = it.next();
                counts.put(entry.key, entry.value);
            }
        }
        return ResponseEntity.ok(counts);
    }
}
