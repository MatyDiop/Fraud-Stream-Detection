#!/usr/bin/env bash
# Installation ponctuelle de l'outil generateurs-v3 (fourni par le prof,
# hors depot git - cf. .gitignore). A relancer seulement si generateurs-v3/
# est recopie sur une nouvelle machine.
#
# Usage : ./scripts/setup-generateurs.sh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GEN_DIR="$ROOT_DIR/generateurs-v3"

if [ ! -d "$GEN_DIR" ]; then
  echo "generateurs-v3/ introuvable a la racine du projet (recuperez-le aupres du prof)." >&2
  exit 1
fi

if ! command -v brew >/dev/null 2>&1; then
  echo "Homebrew requis (macOS) pour installer librdkafka." >&2
  exit 1
fi

echo "==> librdkafka (bibliotheque C requise par confluent-kafka)"
brew list librdkafka >/dev/null 2>&1 || brew install librdkafka

cd "$GEN_DIR"
echo "==> Environnement virtuel Python (.venv)"
python3 -m venv .venv
source .venv/bin/activate

echo "==> Dependances (confluent-kafka)"
CFLAGS="-I$(brew --prefix librdkafka)/include" \
LDFLAGS="-L$(brew --prefix librdkafka)/lib" \
pip install -q -r requirements.txt

echo "==> OK. Verification (dry-run, sans Kafka) :"
python rejouer.py --dossier data/sentinel --dry-run | tail -3
