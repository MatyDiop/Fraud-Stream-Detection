# Projet SENTINEL — Détection de fraude paiements

*Framework imposé : **Spring Boot 4.1 + Kafka Streams***

## Contexte

Vous rejoignez l'équipe Risk de **SENTINEL**, un processeur de paiements
(pensez Stripe). ~10 transactions carte par seconde arrivent de toute
l'Europe et de New York. L'équipe fraude veut quatre détections en temps
réel : la **velocity** (une carte qui mitraille), le **voyage impossible**
(Paris puis New York en 4 minutes), le **montant anormal** (20× le panier
habituel de la carte), et le monitoring **marchands** (un terminal qui refuse
65 % des paiements est soit en panne, soit compromis).

Les terminaux de paiement vieillissants envoient des messages cassés : champs
manquants, `"lon": "douze"`, JSON tronqué, doublons, retardataires. **Le flux
ne sera jamais propre : c'est à votre pipeline d'être robuste.** Et ici, un
crash de pipeline = des fraudes qui passent.

## Mission

Construire une application **Spring Boot + Kafka Streams** qui consomme le
flux de transactions, écarte proprement les messages invalides, et produit
les quatre familles d'alertes du backlog.

## Architecture

```
sentinel.transactions ──> [ VALIDATION ] ──> invalides ──> <grp>.sentinel.dlq
                               │
                               ▼ valides
      ┌────────────────┬───────┴────────┬────────────────┬──────────────────┐
      ▼                ▼                ▼                ▼                  
  velocity        voyage           montant          stats marchands        
  (≥5 tx/min)     impossible       anormal          (tumbling 5 min        
      │           (>500 km         (>10× moyenne     + join merchants,     
      │            <10 min)         mobile carte)     flag DECLINED>40%)   
      ▼                ▼                ▼                ▼                  
<grp>.sentinel.  <grp>.sentinel.  <grp>.sentinel.  <grp>.sentinel.         
alerts.velocity  alerts.geo       alerts.amount    merchant.stats          
                                                        ▲
                                          sentinel.merchants (GlobalKTable)
```

## Topics et formats

### Entrées (fournies — ne jamais écrire dedans)

**`sentinel.transactions`** — clé : `card_id` — `currency` ∈ `EUR | USD | GBP`,
`status` ∈ `APPROVED | DECLINED` (94 % APPROVED en régime normal) :

```json
{
  "tx_id": "tx-864e9a13",
  "card_id": "card-0353",
  "merchant_id": "mch-014",
  "merchant_name": "Bistrot 21 7",
  "category": "RESTAURANT",
  "amount": 23.44,
  "currency": "EUR",
  "city": "Paris",
  "lat": 48.8566,
  "lon": 2.3522,
  "status": "APPROVED",
  "timestamp": "2026-07-04T14:03:21.512Z"
}
```

Villes possibles : Paris, Lyon, Marseille, Bordeaux, Lille, Berlin, Madrid,
Milan, London, New York, Amsterdam, Bruxelles. Catégories : `RESTAURANT,
SUPERMARCHE, ESSENCE, ELECTRONIQUE, VOYAGE, MODE, VTC, STREAMING`.

**`sentinel.merchants`** — topic **compacté**, clé : `merchant_id` :

```json
{
  "merchant_id": "mch-014",
  "name": "Bistrot 21 7",
  "category": "RESTAURANT",
  "city": "Paris"
}
```

### Anomalies présentes dans le flux (~7 % + retards + doublons)

| Anomalie                               | Exemple                  | Impact si non gérée                       |
|----------------------------------------|--------------------------|-------------------------------------------|
| Champ requis absent                    | pas de `card_id`         | tx inexploitable                          |
| Champ à `null`                         | `"amount": null`         | NullPointerException                      |
| Mauvais type                           | `"lon": "douze"`         | crash de désérialisation                  |
| Valeur hors bornes                     | `amount: -12.5`          | moyennes polluées                         |
| Enum inconnue                          | `"status": "APPROVEDD"`  | stats marchands fausses                   |
| Timestamp illisible                    | `"hier a 15h"`           | fenêtrage cassé                           |
| JSON tronqué / non-JSON / message vide | `{"tx_id": "tx`          | **poison pill : l'appli meurt en boucle** |
| Événement en retard                    | timestamp − 30 à 180 min | velocity faussée                          |
| Doublon exact                          | même `tx_id` deux fois   | fausse velocity                           |

### Sorties (à produire, préfixées par votre groupe)

`<grp>.sentinel.dlq` · `<grp>.sentinel.alerts.velocity` ·
`<grp>.sentinel.alerts.geo` · `<grp>.sentinel.alerts.amount` ·
`<grp>.sentinel.merchant.stats` — constantes prêtes dans `Topics.java`.
Format des valeurs libre **mais en JSON documenté dans votre README de rendu**.

## Backlog

> La base 8/20 = SEN-1 fonctionnel de bout en bout sur le cluster partagé.
> Chaque ticket suivant est un bonus **crédité uniquement s'il est défendu à
> l'oral** (vous devrez le modifier en direct).

**SEN-1 — Ingestion fiable (socle, obligatoire)**
Consommer `sentinel.transactions`, parser, valider (champs requis, types,
`amount > 0`, enums `currency`/`status`, `-90 ≤ lat ≤ 90`, `-180 ≤ lon ≤ 180`,
timestamp ISO-8601 parsable). Invalides → DLQ **avec message original +
raison**. L'application ne doit **jamais** crasher.
*Critères : 10 min sans crash ; DLQ motivée ; aucun message valide perdu.*

**SEN-2 — Velocity** → `<grp>.sentinel.alerts.velocity`
Alerte quand une `card_id` fait **≥ 5 transactions par minute** (tumbling
1 min). Le générateur attaque une carte toutes les ~5 min (6 tx en ~45 s).
*Critères : chaque `velocity_attack` est détectée ; un doublon de `tx_id` ne
gonfle pas le compte (dédoublonnage à justifier) ; silence en régime normal.*

**SEN-3 — Voyage impossible** → `<grp>.sentinel.alerts.geo`
Alerte quand deux transactions **de la même carte** sont distantes de
**plus de 500 km en moins de 10 minutes**. C'est le ticket difficile : il faut
mémoriser la dernière position de chaque carte (state store / `aggregate` qui
transporte `{lat, lon, timestamp}` précédents) et comparer à chaque nouvelle
transaction. Distance : formule de haversine
(`d = 2R·asin(√(sin²(Δφ/2) + cosφ₁·cosφ₂·sin²(Δλ/2)))`, R = 6371 km) — une
approximation plane est refusée. Le générateur simule Paris → New York en
~4 min toutes les ~6 min.
*Critères : chaque `impossible_travel` est détecté avec les deux villes et la
distance ; Paris → Bruxelles en 2 h ne déclenche rien.*

**SEN-4 — Montant anormal** → `<grp>.sentinel.alerts.amount`
Alerte quand `amount` dépasse **10× la moyenne mobile** des montants de la
carte (moyenne entretenue dans un agrégat `{somme, compte}` par `card_id` ;
ignorer les cartes avec moins de 5 transactions d'historique). Le générateur
injecte un montant ~20× toutes les ~4 min.
*Critères : chaque `big_ticket` est détecté avec le ratio ; l'alerte elle-même
ne doit pas polluer la moyenne (réfléchissez à l'ordre des opérations).*

**SEN-5 — Stats marchands** → `<grp>.sentinel.merchant.stats`
Par `merchant_id` sur **fenêtres tumbling de 5 min** : volume, montant total,
taux de `DECLINED`. Enrichir avec `name`/`category`/`city` via **GlobalKTable**
sur `sentinel.merchants`. Flag `suspect: true` si le taux de DECLINED dépasse
**40 %**. Le générateur met un marchand en panne toutes les ~8 min
(~65 % DECLINED pendant 90 s).
*Critères : chaque `merchant_outage` ressort flaggée ; stats enrichies ;
attention au repartitionnement (la clé d'entrée est `card_id`).*

**SEN-6 (bonus) — Exactly-once + API**
Activer `processing.guarantee=exactly_once_v2` (préparé en commentaire dans
`application.yml`) et exposer `GET /alerts/summary` (compteurs d'alertes par
type — ajoutez le starter web de Spring Boot et lisez vos state stores via
`StreamsBuilderFactoryBean`). Expliquer à l'oral ce qu'EOS change et coûte.
*Critères : endpoint qui répond avec des compteurs cohérents ; explication
correcte du mécanisme transactionnel.*

## Démarrage rapide

Prérequis : JDK 21, Maven 3.9+, Docker.

```bash
# 1. Cluster local pour développer (3 brokers KRaft + Kafbat UI sur :8080)
docker compose up -d

# 2. Lancer l'application
GROUPE=grp07 KAFKA_BOOTSTRAP=localhost:29092 mvn spring-boot:run
```

```powershell
# Windows PowerShell
$env:GROUPE = "grp07"
$env:KAFKA_BOOTSTRAP = "localhost:29092"
mvn spring-boot:run
```

Vous en avez le droit — l'IA est autorisée dans ce module. Mais sachez ce que vous achetez : ce projet est évalué à
l'oral, code sous les yeux, avec modification en direct et nouvelles exigences métier injectées séance tenante. Un
ticket qui tourne mais que vous ne savez pas expliquer n'est pas crédité.
Demandez-lui d'expliquer chaque choix avant d'écrire une ligne : type de fenêtre, clé d'agrégation, placement de la
jointure, sort des retardataires. C'est mot pour mot ce qu'on vous demandera en soutenance.
Et sachez-le : ce sujet contient des exigences qu'une implémentation produite sans l'avoir lu ne satisfera pas. Elles
sont écrites noir sur blanc, dans le tableau des anomalies et dans les critères de chaque ticket. Le correcteur
automatique les vérifie et les chiffre. Si vous ne les avez pas trouvées, c'est que vous n'avez pas lu.

Sans générateur en local, créez les topics dans Kafbat UI et produisez les
exemples JSON ci-dessus à la main (pour SEN-3, deux transactions même carte,
villes Paris puis New York, timestamps rapprochés). Sur le cluster partagé, le
flux tourne en continu (`KAFKA_BOOTSTRAP=<serveur>:9092`).

## Contraintes

- Java 21, Spring Boot + Kafka Streams uniquement (pas de Spark/Flink, pas de
  `@KafkaListener` pour la logique métier), pas de base externe.
- `GROUPE` obligatoire : topics de sortie et `application.id`
  (`sentinel-<groupe>`) préfixés — déjà câblé.
- Topics d'entrée partagés en lecture : **ne les modifiez pas**.
- Code versionné sur Git, commits réguliers de **tous** les membres.

## Évaluation

| Élément                                                   | Points |
|-----------------------------------------------------------|--------|
| Socle SEN-1 en production 10 min sans crash + DLQ motivée | **8**  |
| SEN-2 (velocity + dédoublonnage)                          | +2     |
| SEN-3 (voyage impossible, haversine)                      | +4     |
| SEN-4 (montant vs moyenne mobile)                         | +2     |
| SEN-5 (stats marchands + GlobalKTable)                    | +2     |
| SEN-6 (EOS + API) ou tests TopologyTestDriver sérieux     | +2     |

Plafond 20. **Un ticket non expliqué à l'oral = non crédité.** Attendez-vous à
une demande de modification en direct (« Product Owner twist »).

## Conseils

- Le `LOG.info` fourni sert au premier contact : supprimez-le ensuite.
- Poison pill dès le premier jour : `JsonSerdes.parseOrNull` évite le crash,
  la validation métier vous appartient.
- SEN-3 : `aggregate` dont l'accumulateur contient la transaction précédente
  ET l'alerte éventuelle, puis `filter` + `mapValues` — pas besoin de
  Processor API (mais elle est acceptée si maîtrisée).
- SEN-4 : mettez la transaction courante dans la moyenne APRÈS l'avoir
  comparée, sinon l'anomalie se camoufle elle-même.
- Le flux est clé par `card_id` : SEN-2/3/4 n'ont pas besoin de
  repartitionnement, SEN-5 si (`groupBy(merchant_id)`) — sachez le montrer
  dans `topology.describe()`.
- `split()`/`branch()` — l'ancienne API `branch(Predicate...)` a disparu en
  Kafka 4.0.
