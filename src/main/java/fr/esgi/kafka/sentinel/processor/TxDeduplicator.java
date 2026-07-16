package fr.esgi.kafka.sentinel.processor;

import fr.esgi.kafka.sentinel.model.Transaction;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

/**
 * Jette les transactions dont le tx_id a deja ete vu (doublon exact).
 * Le README exige qu'un doublon ne gonfle pas le compte de velocity.
 *
 * <p>La cle du flux (card_id) est preservee : on utilise processValues.
 * Le tx_id est memorise dans un state store, avec sa date d'apparition ;
 * un purge periodique supprime les entrees plus vieilles que la retention
 * pour borner la memoire (un store infini exploserait sur un flux continu).
 */
public class TxDeduplicator implements FixedKeyProcessor<String, Transaction, Transaction> {

    private final String storeName;
    private final long retentionMs;
    private KeyValueStore<String, Long> seen;
    private FixedKeyProcessorContext<String, Transaction> context;

    public TxDeduplicator(String storeName, Duration retention) {
        this.storeName = storeName;
        this.retentionMs = retention.toMillis();
    }

    @Override
    public void init(FixedKeyProcessorContext<String, Transaction> context) {
        this.context = context;
        this.seen = context.getStateStore(storeName);
        // Purge basee sur le temps evenement (stream time), pas l'horloge murale :
        // coherent avec le rejeu qui repose les timestamps d'origine.
        context.schedule(Duration.ofMinutes(1), PunctuationType.STREAM_TIME, this::purge);
    }

    @Override
    public void process(FixedKeyRecord<String, Transaction> record) {
        String txId = record.value().txId();
        if (seen.get(txId) != null) {
            return; // doublon -> on ne forward pas
        }
        seen.put(txId, record.timestamp());
        context.forward(record);
    }

    private void purge(long now) {
        try (KeyValueIterator<String, Long> it = seen.all()) {
            while (it.hasNext()) {
                KeyValue<String, Long> entry = it.next();
                if (now - entry.value > retentionMs) {
                    seen.delete(entry.key);
                }
            }
        }
    }
}
