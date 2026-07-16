# Générateurs de données — Projets Kafka ESGI

Neuf générateurs de flux temps réel, un par projet. Chaque flux mélange :

- des **données réalistes** (popularité zipfienne, sessions, paiements différés, géolocalisation…),
- des **données sales** comme en production : champs manquants ou `null`, types invalides (`"amount": "douze"`), valeurs hors bornes, enums inconnues, timestamps illisibles, JSON tronqué, messages vides, non-JSON, **doublons** et **événements en retard** (30 à 180 min),
- des **incidents scriptés périodiques** que les étudiants doivent détecter (fraude, surge, bot, buffering storm…). Chaque déclenchement est loggé sur stdout avec le préfixe `[incident]` : c'est votre corrigé en temps réel pour vérifier que les alertes des groupes partent au bon moment.

| Scénario | Inspiration | Topics produits |
|---|---|---|
| `binge` | Netflix | `binge.playback.events`, `binge.catalog` (compacté) |
| `cartflow` | Amazon | `cartflow.orders`, `cartflow.payments`, `cartflow.stock.movements` |
| `dispatch` | Uber | `dispatch.ride.requests`, `dispatch.driver.locations`, `dispatch.trip.events` |
| `viral` | Instagram | `viral.posts`, `viral.interactions` |
| `tempo` | Spotify | `tempo.listening.events`, `tempo.tracks` (compacté) |
| `sentinel` | Stripe | `sentinel.transactions`, `sentinel.merchants` (compacté) |
| `relay` | Criteo | `relay.impressions`, `relay.clicks`, `relay.campaigns` (compacté) |
| `arena` | Riot Games | `arena.match.events`, `arena.purchases`, `arena.players` (compacté) |
| `forge` | Siemens MindSphere | `forge.readings`, `forge.production`, `forge.machines` (compacté) |

Le détail des incidents (période, seuils à détecter) est dans `PROJETS-VUE-ENSEMBLE.md`.

## Installation locale

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Utilisation

### Rejouer un jeu de données vers Kafka

`rejouer.py` pousse un dossier `--to-dir` dans un cluster, tous topics
fusionnés dans l'ordre chronologique, en reposant le `ts_ms` d'origine comme
timestamp du record : la relation entre temps événement et temps d'ingestion
est exactement celle de la génération.

```bash
# aussi vite que possible (défaut) : 3 h de flux en une minute
python rejouer.py --dossier data/arena --bootstrap localhost:29092

# en créant les topics au passage (broker unique : RF 1)
python rejouer.py --dossier data/arena --project arena --create-topics \
    --replication-factor 1

# en temps réel, comme si le générateur tournait (--speed 60 = 60x)
python rejouer.py --dossier data/arena --speed 1
```

## Docker (recommandé sur votre serveur)

```bash
# Les 9 générateurs d'un coup
KAFKA_BOOTSTRAP=IP-DU-SERVEUR:9092 docker compose up -d --build

# Un seul projet
KAFKA_BOOTSTRAP=IP-DU-SERVEUR:9092 docker compose up -d --build gen-binge

# Suivre les incidents (= corrigé temps réel)
docker compose logs -f | grep incident
```

Chaque service tente de créer ses topics au démarrage (`--create-topics`) ; si les topics existent déjà, l'erreur est ignorée. `RF=1` si votre serveur n'a qu'un broker.

## Points d'attention

- **Volumétrie** : ~60 evt/s tous scénarios confondus aux débits par défaut, rétention 24 h sur les topics d'événements → très raisonnable pour un serveur perso.
- **Clés** : chaque topic est produit avec une clé métier pertinente (user, carte, SKU, driver, post…) — le partitionnement est donc réaliste et les étudiants devront parfois re-partitionner (`selectKey`/`groupBy`).
- Les topics `*.catalog`, `tempo.tracks`, `sentinel.merchants`, `relay.campaigns`, `arena.players` et `forge.machines` sont **compactés** et réémis périodiquement : parfaits pour les jointures `GlobalKTable`.
- Les événements **en retard** gardent un JSON valide : ils doivent passer la validation mais être gérés côté fenêtrage (grace period) — c'est voulu.
