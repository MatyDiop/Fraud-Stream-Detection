"""Utilitaires partages entre les scenarios."""
import heapq
import time
from datetime import datetime

# --- Horloge des scenarios -------------------------------------------------
# Reelle par defaut. Le generateur bascule sur une horloge virtuelle en mode
# hors-ligne (--to-dir) : des heures de flux se simulent en quelques secondes.
# Les scenarios appellent common.monotonic() et ignorent tout du mode actif.
_VIRTUELLE = {"t": None, "dt": None}


def monotonic():
    """Temps monotone des scenarios, en secondes."""
    t = _VIRTUELLE["t"]
    return time.monotonic() if t is None else t


def caler_horloge(t, dt=None):
    """Fixe l'horloge virtuelle. None = retour a l'horloge reelle.

    dt (datetime simule) sert a horodater les annonces d'incident : en mode
    hors-ligne, l'heure murale n'a aucun sens, c'est l'heure SIMULEE qui
    permet de retrouver l'incident dans le jeu de donnees.
    """
    _VIRTUELLE["t"] = t
    _VIRTUELLE["dt"] = dt


def iso(dt):
    """Format ISO-8601 UTC avec millisecondes : 2026-07-04T12:34:56.789Z"""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + ("%03dZ" % (dt.microsecond // 1000))


def zipf_weights(n, s=1.15):
    """Popularite realiste : quelques items tres populaires, longue traine."""
    return [1.0 / ((i + 1) ** s) for i in range(n)]


def weighted_index(rng, weights):
    total = sum(weights)
    x = rng.random() * total
    acc = 0.0
    for i, w in enumerate(weights):
        acc += w
        if x <= acc:
            return i
    return len(weights) - 1


def pick_weighted(rng, pairs):
    """pairs = [(valeur, poids), ...]"""
    total = sum(w for _, w in pairs)
    x = rng.random() * total
    acc = 0.0
    for value, w in pairs:
        acc += w
        if x <= acc:
            return value
    return pairs[-1][0]


def new_id(rng, prefix):
    return "%s-%08x" % (prefix, rng.getrandbits(32))


def announce(scenario, name, detail):
    dt = _VIRTUELLE["dt"] or datetime.now()
    ts = dt.strftime("%H:%M:%S")
    print("[incident] %s %s :: %s :: %s" % (ts, scenario, name, detail), flush=True)


class Incident:
    """Incident periodique : demarre toutes les `period` secondes environ,
    dure `duration` secondes. active() -> (est_actif, vient_de_demarrer)."""

    def __init__(self, name, period, duration, rng, first_after=None):
        self.name = name
        self.period = period
        self.duration = duration
        self.rng = rng
        base = first_after if first_after is not None else period * 0.4
        self.next_start = monotonic() + base
        self.end = 0.0

    def active(self):
        t = monotonic()
        started = False
        if t >= self.next_start:
            jitter = self.period * (0.85 + 0.3 * self.rng.random())
            self.next_start = t + jitter
            self.end = t + self.duration
            started = True
        return t < self.end, started


class FutureQueue:
    """File d'evenements planifies dans le futur (paiements, fins de course...)."""

    def __init__(self):
        self._heap = []
        self._seq = 0

    def push(self, fire_at, item):
        self._seq += 1
        heapq.heappush(self._heap, (fire_at, self._seq, item))

    def pop_due(self, now):
        due = []
        while self._heap and self._heap[0][0] <= now:
            _, _, item = heapq.heappop(self._heap)
            due.append(item)
        return due
