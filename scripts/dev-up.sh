#!/usr/bin/env bash
# Demarre l'environnement de dev local complet en une commande :
#   1. cluster Kafka local (3 brokers + Kafbat UI)
#   2. topics sentinel.transactions / sentinel.merchants
#   3. injection du jeu de donnees fige (114k transactions + 720 fiches marchand)
#
# Usage : ./scripts/dev-up.sh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GEN_DIR="$ROOT_DIR/generateurs-v3"
BOOTSTRAP="localhost:29092"

if [ ! -d "$GEN_DIR" ]; then
  echo "generateurs-v3/ introuvable a la racine du projet." >&2
  exit 1
fi

echo "==> 1/3 Cluster Kafka local (docker compose)"
cd "$ROOT_DIR"
docker compose up -d

echo "==> Attente que les brokers soient prets..."
for i in $(seq 1 30); do
  if docker exec kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh \
      --bootstrap-server localhost:9092 >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "==> Nettoyage des topics existants (evite le doublement des donnees)"
# Sans ca, relancer dev-up.sh sans 'docker compose down' ferait rejouer
# par-dessus les donnees deja presentes (chaque message compterait 2x).
# On supprime les topics d'entree + toutes les sorties/etats du groupe.
for topic in sentinel.transactions sentinel.merchants \
    $(docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 --list 2>/dev/null \
        | grep -E "^grp00\.|^sentinel-grp00-"); do
  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --delete --topic "$topic" >/dev/null 2>&1 || true
done
sleep 2

echo "==> 2/3 + 3/3 Creation des topics et injection du jeu de donnees SENTINEL"
cd "$GEN_DIR"
if [ ! -d ".venv" ]; then
  echo "Environnement Python introuvable : lancez d'abord scripts/setup-generateurs.sh" >&2
  exit 1
fi
source .venv/bin/activate
python rejouer.py --dossier data/sentinel --project sentinel \
    --create-topics --replication-factor 1 --bootstrap "$BOOTSTRAP"

echo ""
echo "==> Pret. Kafbat UI : http://localhost:8081"
echo "==> Lancer l'appli  : GROUPE=grp00 KAFKA_BOOTSTRAP=$BOOTSTRAP mvn spring-boot:run"
