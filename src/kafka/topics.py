from __future__ import annotations

from models.events import Provider

# Nombres explícitos, creados por kafka-init en docker-compose.yml — no
# depender de auto.create.topics (defaults poco predecibles de particiones).
TOPIC_PULPEY = "campaigns.pulpey"
TOPIC_TIGO_GT = "campaigns.tigo.gt"
TOPIC_CLARO_GT = "campaigns.claro.gt"
TOPIC_TIGO_HN = "campaigns.tigo.hn"
TOPIC_CLARO_HN = "campaigns.claro.hn"
TOPIC_TIGO_SV = "campaigns.tigo.sv"
TOPIC_CLARO_SV = "campaigns.claro.sv"
TOPIC_TIGO_NI = "campaigns.tigo.ni"
TOPIC_CLARO_NI = "campaigns.claro.ni"
TOPIC_TIGO_CR = "campaigns.tigo.cr"
TOPIC_CLARO_CR = "campaigns.claro.cr"
TOPIC_DELIVERY_STATUS = "campaigns.delivery-status"
# Destino de mensajes poison (ver kafka/consumer_base.py) — un solo topic
# compartido para todos los topics de arriba, no uno por topic: el volumen
# esperado de mensajes poison es bajo y un solo lugar para revisarlos alcanza
# mientras no haya operación real. El topic/partition/offset original queda
# en el envelope publicado, así que sigue siendo identificable de dónde vino.
TOPIC_DEAD_LETTER = "campaigns.dead-letter"

PROVIDER_TOPICS: dict[Provider, str] = {
    "pulpey": TOPIC_PULPEY,
    "tigo_gt": TOPIC_TIGO_GT,
    "claro_gt": TOPIC_CLARO_GT,
    "tigo_hn": TOPIC_TIGO_HN,
    "claro_hn": TOPIC_CLARO_HN,
    "tigo_sv": TOPIC_TIGO_SV,
    "claro_sv": TOPIC_CLARO_SV,
    "tigo_ni": TOPIC_TIGO_NI,
    "claro_ni": TOPIC_CLARO_NI,
    "tigo_cr": TOPIC_TIGO_CR,
    "claro_cr": TOPIC_CLARO_CR,
}


def topic_for_provider(provider: Provider) -> str:
    return PROVIDER_TOPICS[provider]
