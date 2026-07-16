package fr.esgi.kafka.sentinel;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Cree les 5 topics de sortie au demarrage, de facon idempotente.
 *
 * <p>Le {@code KafkaAdmin} de Spring Boot applique ces {@link NewTopic} au
 * lancement : il cree les topics manquants et ignore ceux qui existent deja
 * (aucune modification, aucune erreur). Indispensable sur un cluster de
 * production ou l'auto-creation des topics est desactivee : sans ca, la
 * premiere ecriture d'une alerte echouerait faute de topic.
 *
 * <p>Partitions alignees sur le topic d'entree {@code sentinel.transactions}
 * (6). Facteur de replication a 1 pour rester valable sur n'importe quel
 * cluster (un seul broker suffit) ; a monter si le cluster cible l'impose.
 */
@Configuration
public class OutputTopicsConfig {

    private static final int PARTITIONS = 6;
    private static final short REPLICATION = 1;

    private static NewTopic topic(String name) {
        return TopicBuilder.name(name)
                .partitions(PARTITIONS)
                .replicas(REPLICATION)
                .build();
    }

    @Bean
    public NewTopic dlqTopic() {
        return topic(Topics.DLQ);
    }

    @Bean
    public NewTopic velocityTopic() {
        return topic(Topics.ALERTS_VELOCITY);
    }

    @Bean
    public NewTopic geoTopic() {
        return topic(Topics.ALERTS_GEO);
    }

    @Bean
    public NewTopic amountTopic() {
        return topic(Topics.ALERTS_AMOUNT);
    }

    @Bean
    public NewTopic merchantStatsTopic() {
        return topic(Topics.MERCHANT_STATS);
    }
}
