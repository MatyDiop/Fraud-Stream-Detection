package fr.esgi.kafka.sentinel.common;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

/** Serdes JSON generiques (fournis). */
public final class JsonSerdes {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private JsonSerdes() {
    }

    /** Serde type pour les etapes internes du pipeline (apres validation). */
    public static <T> Serde<T> of(Class<T> type) {
        Serializer<T> ser = (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Serialisation JSON impossible", e);
            }
        };
        Deserializer<T> de = (topic, bytes) -> {
            if (bytes == null) {
                return null;
            }
            try {
                return MAPPER.readValue(bytes, type);
            } catch (Exception e) {
                throw new RuntimeException("JSON invalide", e);
            }
        };
        return Serdes.serdeFrom(ser, de);
    }

    /**
     * Parse "sur" : renvoie null si le JSON est illisible au lieu de lever
     * une exception. Utile pour trier valide / invalide (DLQ) sans faire
     * tomber l'application sur un poison pill.
     * Attention : un JSON lisible n'est pas forcement un evenement VALIDE
     * (champs manquants, valeurs hors bornes...) - cette partie de la
     * validation vous appartient.
     */
    public static <T> T parseOrNull(String json, Class<T> type) {
        if (json == null) {
            return null;
        }
        try {
            return MAPPER.readValue(json, type);
        } catch (Exception e) {
            return null;
        }
    }

    public static String toJson(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (Exception e) {
            throw new RuntimeException("Serialisation JSON impossible", e);
        }
    }
}
