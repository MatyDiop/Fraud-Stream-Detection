package fr.esgi.kafka.sentinel;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Point d'entree (fourni). La topologie se construit dans
 * {@link SentinelTopology}. Lancement :
 *
 *   GROUPE=grp07 KAFKA_BOOTSTRAP=localhost:29092 mvn spring-boot:run
 */
@SpringBootApplication
public class SentinelApplication {

    public static void main(String[] args) {
        SpringApplication.run(SentinelApplication.class, args);
    }
}
