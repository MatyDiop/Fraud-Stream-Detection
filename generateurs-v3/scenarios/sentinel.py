"""Scenario SENTINEL - paiements par carte / anti-fraude (inspire Stripe).

"""
import random
from . import common

NAME = "sentinel"
DEFAULT_RATE = 10.0
CATALOG_REFRESH = 600

TOPIC_TX = "sentinel.transactions"
TOPIC_MERCHANTS = "sentinel.merchants"

TOPICS = {
    TOPIC_TX: {"partitions": 6, "config": {"retention.ms": "86400000"}},
    TOPIC_MERCHANTS: {"partitions": 3, "config": {"cleanup.policy": "compact"}},
}
