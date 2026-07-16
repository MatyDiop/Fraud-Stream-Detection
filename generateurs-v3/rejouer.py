#!/usr/bin/env python3
"""Rejoue vers Kafka un jeu de donnees produit par generator.py --to-dir.

Les fichiers sont des .jsonl {key, value, ts_ms}, un par topic. Le rejeu
fusionne tous les topics par ordre chronologique et repose le ts_ms
d'origine comme timestamp du record Kafka : le temps evenement et le
temps d'ingestion gardent exactement la relation qu'ils avaient a la
generation (les retardataires restent en retard).

Exemples :
  # aussi vite que possible (defaut) : 3 h de flux en une minute
  python rejouer.py --dossier data/arena --bootstrap localhost:29092

  # creer les topics du projet avant de pousser (broker unique : RF 1)
  python rejouer.py --dossier data/arena --project arena --create-topics \\
      --replication-factor 1

  # en temps reel, comme si le generateur tournait (--speed 60 = 60x)
  python rejouer.py --dossier data/arena --speed 1
"""
import argparse
import glob
import heapq
import importlib
import json
import os
import sys
import time


def lire(chemin, topic):
    with open(chemin, encoding="utf-8") as f:
        for ligne in f:
            ligne = ligne.strip()
            if not ligne:
                continue
            d = json.loads(ligne)
            yield (int(d.get("ts_ms") or 0), topic, d.get("key"),
                   d.get("value", ""))


def creer_topics(projet, bootstrap, rf):
    from confluent_kafka.admin import AdminClient, NewTopic
    scenario = importlib.import_module("scenarios.%s" % projet)
    admin = AdminClient({"bootstrap.servers": bootstrap})
    futures = admin.create_topics([
        NewTopic(nom, num_partitions=cfg.get("partitions", 3),
                 replication_factor=rf, config=cfg.get("config", {}))
        for nom, cfg in scenario.TOPICS.items()])
    for nom, f in futures.items():
        try:
            f.result()
            print("[topics] cree : %s" % nom)
        except Exception as e:
            print("[topics] %s : %s" % (nom, e))


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dossier", required=True,
                    help="dossier produit par generator.py --to-dir")
    ap.add_argument("--bootstrap",
                    default=os.environ.get("KAFKA_BOOTSTRAP",
                                           "localhost:29092"))
    ap.add_argument("--speed", type=float, default=0,
                    help="0 (defaut) = aussi vite que possible ; "
                         "1 = temps reel ; 60 = 60 fois plus vite")
    ap.add_argument("--project", default=None,
                    help="scenario, requis seulement avec --create-topics")
    ap.add_argument("--create-topics", action="store_true")
    ap.add_argument("--replication-factor", type=int, default=3)
    ap.add_argument("--dry-run", action="store_true",
                    help="afficher au lieu de produire")
    args = ap.parse_args()

    fichiers = sorted(glob.glob(os.path.join(args.dossier, "*.jsonl")))
    if not fichiers:
        raise SystemExit("aucun .jsonl dans %s" % args.dossier)

    if args.create_topics:
        if not args.project:
            raise SystemExit("--create-topics exige --project")
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        creer_topics(args.project, args.bootstrap, args.replication_factor)

    flux = [lire(f, os.path.basename(f)[:-6]) for f in fichiers]
    print("[rejeu] %d topics : %s"
          % (len(fichiers),
             ", ".join(os.path.basename(f)[:-6] for f in fichiers)))

    if args.dry_run:
        producer = None
    else:
        from confluent_kafka import Producer
        producer = Producer({"bootstrap.servers": args.bootstrap,
                             "linger.ms": 50, "compression.type": "lz4"})

    n = 0
    t0_flux = None
    t0_reel = time.monotonic()
    par_topic = {}
    # heapq.merge : fusion chronologique en flux, sans tout charger en memoire
    for ts_ms, topic, key, value in heapq.merge(*flux, key=lambda r: r[0]):
        if args.speed > 0:
            if t0_flux is None:
                t0_flux = ts_ms
            cible = (ts_ms - t0_flux) / 1000.0 / args.speed
            retard = cible - (time.monotonic() - t0_reel)
            if retard > 0:
                time.sleep(retard)
        if producer is None:
            print("%s | %s | %s" % (topic, key, value[:110]))
        else:
            producer.produce(topic,
                             key=(key or "").encode("utf-8"),
                             value=value.encode("utf-8"),
                             timestamp=ts_ms)
            producer.poll(0)
        n += 1
        par_topic[topic] = par_topic.get(topic, 0) + 1
        if n % 20000 == 0:
            print("[rejeu] %d messages..." % n)

    if producer is not None:
        producer.flush(30)
    for topic in sorted(par_topic):
        print("[rejeu] %-38s %7d messages" % (topic, par_topic[topic]))
    print("[rejeu] termine : %d messages en %.1f s"
          % (n, time.monotonic() - t0_reel))


if __name__ == "__main__":
    main()
