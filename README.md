# SENTINEL — Détection de fraude paiements en temps réel

Pipeline **Spring Boot 4 + Kafka Streams** (Java 21) qui consomme un flux de
transactions carte (~10 tx/s, ~7 % de messages cassés, retards, doublons),
écarte proprement les invalides, et produit quatre familles d'alertes fraude.

> Sujet complet : [SUJET.md](SUJET.md). Backlog SEN-1 à SEN-6 : tous implémentés.

## Démarrage rapide

### Prérequis

JDK 21+, Maven 3.9+, Docker. Pour le rejeu local : Python 3 + le dossier
`generateurs-v3/` fourni par l'enseignant (hors dépôt, cf. `.gitignore`),
installé une fois via `./scripts/setup-generateurs.sh`.

### En local (cluster Docker + jeu de données figé)

> `grp00` est une valeur de **test local uniquement** (câblée dans
> `./scripts/dev-up.sh`). Ce n'est **pas** le vrai groupe — ne jamais
> l'utiliser sur le cluster partagé (cf. section suivante).

**L'ordre est obligatoire.** L'étape 1 crée les topics d'entrée
(`sentinel.transactions`, `sentinel.merchants`) et y injecte les données.
Sans elle, l'application démarre puis **plante immédiatement** au chargement
de la GlobalKTable (voir [Dépannage](#dépannage)).

```bash
# 1. D'ABORD : cluster local (3 brokers KRaft + Kafbat UI sur :8081)
#    + création des topics d'entrée + injection des 114 538 transactions
#    du jeu figé. Idempotent (relançable sans doublonner). Attendre "Pret".
./scripts/dev-up.sh

# 2. ENSUITE, dans le même terminal : l'application
GROUPE=grp00 KAFKA_BOOTSTRAP=localhost:29092 mvn spring-boot:run
```

Prérequis Java : le terminal doit utiliser un **JDK ≥ 21** (`java -version`).
Sinon, préfixer par `JAVA_HOME=/opt/homebrew/opt/openjdk@21` (macOS Homebrew).

Vérifier : Kafbat UI sur <http://localhost:8081> (topics `grp00.sentinel.*`),
API sur <http://localhost:8090/alerts/summary>.

Pour tout arrêter : `Ctrl+C` sur l'application, puis `docker compose down`.

### Sur le cluster partagé (évaluation) — GROUPE = grp07

```bash
GROUPE=grp07 KAFKA_BOOTSTRAP=<serveur-du-prof>:9092 mvn spring-boot:run
```

Sorties produites sous `grp07.sentinel.*` (DLQ, alertes, stats marchands).
Ne jamais oublier `GROUPE` ici : sans lui, `Topics.java` retombe sur le
défaut `grp00` (valeur de test) et le correcteur ne trouverait aucune sortie
sous le bon groupe.

`GROUPE` préfixe les 5 topics de sortie et l'`application.id`
(`sentinel-<groupe>`) — câblé dans `Topics.java`. Les topics d'entrée
(`sentinel.transactions`, `sentinel.merchants`) sont consommés en lecture
seule, jamais écrits.

### Tests

```bash
mvn test        # 8 tests TopologyTestDriver, sans broker
```

Chaque test reprend un critère écrit du sujet : montant négatif → DLQ motivée,
poison pill sans crash, aucun valide perdu, 5 tx/min → une alerte velocity,
doublon de `tx_id` non compté, Paris→NYC en 4 min → `impossible_travel`,
Paris→Bruxelles en 2 h → silence, retardataire → pas de fausse alerte géo.

## Architecture

```
sentinel.transactions ──► VALIDATION (JsonNode, champ par champ)
                              │
              invalides ──────┴──────► <grp>.sentinel.dlq  {reason, raw}
                              │ valides (KStream<card_id, Transaction>)
        ┌─────────────┬───────┴───────┬────────────────┐
        ▼             ▼               ▼                ▼
   SEN-2 velocity  SEN-3 geo     SEN-4 amount    SEN-5 merchants
   dédup tx_id     aggregate     aggregate       groupBy(merchant_id)
   tumbling 1min   {position     {somme,compte}  = REPARTITION
   + grace 2min    précédente}   comparer AVANT  tumbling 5min
   count ≥ 5       haversine     d'incorporer    + leftJoin GlobalKTable
   + suppress      >500km ET     ratio > 10×     (sentinel.merchants)
        │          <10min             │          taux DECLINED > 40 %
        ▼             ▼               ▼                ▼
   alerts.velocity alerts.geo   alerts.amount   merchant.stats
        └─────────────┴───────┬───────┴────────────────┘
                              ▼
               SEN-6 : count par type (state store)
               ──► GET /alerts/summary (Interactive Queries)
```

Exactly-once (`processing.guarantee=exactly_once_v2`) actif sur toute la
topologie : lecture + mise à jour des state stores + écriture des alertes
sont atomiques.

## Formats de sortie (JSON)

### `<grp>.sentinel.dlq` — clé : celle du message d'origine

Tout message invalide, avec le brut intact (rejouable) et une raison lisible.
21 raisons distinctes observées sur le jeu figé (7 597 rejets / 114 538).

```json
{"reason": "amount <= 0", "raw": "{\"tx_id\": \"tx-...\", \"amount\": -12.5, ...}"}
```

Raisons émises : `JSON illisible`, `champ requis manquant: <champ>`,
`amount: type invalide`, `amount <= 0`, `currency inconnue: <val>`,
`status inconnu: <val>`, `lat/lon: type invalide`, `lat hors bornes`,
`lon hors bornes`, `timestamp non ISO-8601`.

### `<grp>.sentinel.alerts.velocity` — clé : `card_id`

Une alerte par (carte, fenêtre d'1 min) dont le compte dédoublonné atteint 5.

```json
{"type": "velocity_attack", "card_id": "card-0405", "count": 5,
 "window_start": "2026-07-15T22:55:00Z", "window_end": "2026-07-15T22:56:00Z"}
```

### `<grp>.sentinel.alerts.geo` — clé : `card_id`

Deux transactions de la même carte à plus de 500 km d'écart en moins de
10 min (haversine, timestamps embarqués).

```json
{"type": "impossible_travel", "card_id": "card-0115",
 "from_city": "Paris", "to_city": "New York",
 "distance_km": 5837.2, "minutes": 4.0,
 "from_ts": "2026-07-15T22:54:12.542Z", "to_ts": "2026-07-15T22:58:12.542Z"}
```

### `<grp>.sentinel.alerts.amount` — clé : `card_id`

Montant > 10× la moyenne mobile de la carte (≥ 5 transactions d'historique).

```json
{"type": "big_ticket", "card_id": "card-0051", "amount": 485.85,
 "average": 32.48, "ratio": 15.0, "tx_id": "tx-dcd744ca",
 "timestamp": "2026-07-15T21:49:52.042Z"}
```

### `<grp>.sentinel.merchant.stats` — clé : `merchant_id`

Stats par marchand et fenêtre tumbling de 5 min, enrichies via GlobalKTable.
`type` vaut `merchant_outage` quand `suspect` est vrai (taux DECLINED > 40 %).

```json
{"type": "merchant_stats", "merchant_id": "mch-000", "name": "Chez Momo 8",
 "category": "VTC", "city": "Amsterdam", "volume": 3, "total_amount": 174.35,
 "declined_rate": 0.0, "suspect": false,
 "window_start": "2026-07-15T22:50:00Z", "window_end": "2026-07-15T22:55:00Z"}
```

### `GET /alerts/summary` (port 8090)

Compteurs d'alertes par type, lus dans le state store `sen6-alert-counts`
via Interactive Queries.

```json
{"velocity_attack": 606, "impossible_travel": 62039, "big_ticket": 117, "merchant_outage": 27}
```

## Choix de conception (par ticket)

**SEN-1.** Parse en `JsonNode` puis validation champ par champ — jamais de
mapping direct vers `Transaction` (coercions Jackson silencieuses : un
`"amount": "23.44"` en *string* passerait) et jamais d'exception (poison
pill). On valide exactement la liste du sujet, rien de plus : rejeter une
ville ou catégorie inconnue perdrait des messages valides.

**SEN-2.** Clé déjà `card_id` → `groupByKey` sans repartition. Dédoublonnage
des `tx_id` **avant** le comptage (state store + purge par ponctuation
STREAM_TIME, rétention 10 min — le jeu contient 2 065 doublons).
Fenêtre tumbling 1 min, **grace 2 min** : les retardataires de 30-180 min
sont valides mais ne rouvrent pas une fenêtre de vélocité ancienne (WARN
`Skipping record for expired window` = comportement voulu).
`suppress(untilWindowCloses)` : une seule alerte finale par fenêtre.

**SEN-3.** `aggregate` par carte transportant `{lat, lon, city, ts}`
précédents + l'alerte éventuelle. Distance de **haversine** (R = 6371 km,
l'approximation plane est refusée). Condition **ET** : > 500 km ET < 10 min
sur les **timestamps embarqués**. Retardataire (Δt < 0) : ni comparaison ni
écrasement de la position récente → pas de fausse alerte.
`withCachingDisabled()` pour qu'aucun état porteur d'alerte ne soit écrasé
avant émission.

**SEN-4.** `aggregate {somme, compte}` par carte. **Comparer avant
d'incorporer** : un montant 20× incorporé d'abord gonflerait sa propre
moyenne et se camouflerait. Cartes avec < 5 transactions ignorées.

**SEN-5.** `groupBy(merchant_id)` alors que la clé du flux est `card_id` →
**topic de repartition** créé (visible dans `topology.describe()`).
Enrichissement en **`leftJoin`** sur **GlobalKTable** (table répliquée, pas
de repartition pour la jointure ; un marchand hors référentiel ne perd pas
ses stats). Jointure faite au niveau du flux de stats, conformément à
« la jointure avant l'agrégat » pour la clé de regroupement (ici la clé
`merchant_id` est portée par l'événement lui-même).

**SEN-6.** `exactly_once_v2` : commits transactionnels (latence et débit en
retrait contre la garantie « chaque alerte exactement une fois » — vérifié :
`read_committed` = `read_uncommitted` sur toutes les sorties). API REST par
Interactive Queries sur le store `sen6-alert-counts` (503 tant que le stream
n'est pas RUNNING).

## Limites connues et assumées

- **SEN-3 est volumineux (~62 000 alertes sur le jeu figé)** : le générateur
  tire une ville aléatoire par transaction, donc 61,5 % des paires
  consécutives dépassent déjà 500 km / 10 min. La règle littérale du sujet
  est implémentée telle quelle ; en production réelle on ajouterait un modèle
  de localisation habituelle par carte. En attente du détail des seuils
  (`PROJETS-VUE-ENSEMBLE.md`) pour recalibrer si nécessaire.
- **SEN-5 flagge des marchands à très faible volume** (2 tx dont 1 refusée =
  50 % > 40 %). Règle littérale conservée ; un seuil de significativité
  (volume ≥ 10, parallèle du « ≥ 5 tx » de SEN-4) l'éliminerait.
- Les montants aberrants mais positifs (`1e9`) passent SEN-1 (la règle est
  `amount > 0`, sans borne haute — ne pas sur-valider) et ressortent en
  SEN-4 avec un ratio astronomique : comportement cohérent inter-tickets.

## Structure du code

```
src/main/java/fr/esgi/kafka/sentinel/
├── SentinelTopology.java        # toute la topologie, un bloc par ticket
├── Topics.java                  # noms des topics (fourni, non modifié)
├── api/AlertsSummaryController  # SEN-6 : GET /alerts/summary
├── common/Geo.java              # haversine
├── common/JsonSerdes.java       # serdes JSON (fourni)
├── model/                       # records : Transaction, Merchant (fournis),
│                                #   DlqEntry, VelocityAlert, GeoAlert/State,
│                                #   AmountAlert/State, MerchantStats*/Out
├── processor/TxDeduplicator     # dédoublonnage tx_id (state store + purge)
└── validation/                  # TransactionValidator, ValidationResult
```

## Scripts

| Script | Rôle |
|---|---|
| `scripts/setup-generateurs.sh` | Install ponctuelle de l'outil de rejeu (venv + librdkafka) |
| `scripts/dev-up.sh` | Cluster local + topics + injection du jeu figé (idempotent) |

Rejeu manuel (depuis `generateurs-v3/`, venv activé) :
`python rejouer.py --dossier data/sentinel --bootstrap localhost:29092`
(`--speed 60` pour un rejeu en temps quasi réel, `--create-topics --project
sentinel --replication-factor 1` après un `docker compose down`).

## Dépannage

**`There are no partitions available for topic sentinel.merchants`**
(au démarrage, `BUILD FAILURE`, exit 1). Le topic d'entrée n'existe pas :
l'application a été lancée **sans** avoir fait `./scripts/dev-up.sh` avant,
ou le cluster a été recréé (`docker compose down`/redémarrage) sans réinjecter.
→ En local : (re)lancer `./scripts/dev-up.sh` puis l'application. Sur le
cluster partagé, ce topic est fourni en continu par le générateur du prof :
l'erreur n'y arrive pas.

**`UnsupportedClassVersionError ... class file version 65.0 ... up to 61.0`**
Le terminal utilise un JDK trop vieux (61 = Java 17) pour du code compilé en
Java 21 (65). → Vérifier `java -version` (doit être ≥ 21) ; sinon préfixer par
`JAVA_HOME=/opt/homebrew/opt/openjdk@21`.

**`The JAVA_HOME environment variable is not defined correctly`**
`JAVA_HOME` pointe vers un dossier qui n'existe plus (ancien JDK supprimé). →
Le corriger vers un JDK ≥ 21 installé, ou rouvrir un terminal si le profil a
été mis à jour.

**Les brokers Kafka crashent (`Exited 1`) en local sous forte charge.**
Manque de mémoire Docker (ex. Airflow/autres conteneurs tournent en parallèle).
→ Libérer de la mémoire (arrêter les conteneurs inutilisés) avant de relancer
`./scripts/dev-up.sh`. Non pertinent sur le cluster dédié du prof.
