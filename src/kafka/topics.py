from __future__ import annotations

from models.events import Provider

# Nombres explícitos, creados por kafka-init en docker-compose.yml — no
# depender de auto.create.topics (defaults poco predecibles de particiones).
TOPIC_PULPEY = "campaigns.pulpey"
TOPIC_TIGO_GT = "campaigns.tigo.gt"
TOPIC_CLARO_GT = "campaigns.claro.gt"
TOPIC_DELIVERY_STATUS = "campaigns.delivery-status"

PROVIDER_TOPICS: dict[Provider, str] = {
    "pulpey": TOPIC_PULPEY,
    "tigo_gt": TOPIC_TIGO_GT,
    "claro_gt": TOPIC_CLARO_GT,
}


def topic_for_provider(provider: Provider) -> str:
    return PROVIDER_TOPICS[provider]
