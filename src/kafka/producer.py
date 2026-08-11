from __future__ import annotations

import json

from aiokafka import AIOKafkaProducer

from config.settings import settings

_producer: AIOKafkaProducer | None = None


async def start_producer() -> None:
    global _producer
    _producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
    )
    await _producer.start()


async def stop_producer() -> None:
    global _producer
    if _producer is not None:
        await _producer.stop()
        _producer = None


def get_producer() -> AIOKafkaProducer:
    if _producer is None:
        raise RuntimeError("Kafka producer not started — start_producer() must run in the app lifespan")
    return _producer


async def publish(topic: str, key: str, value: dict) -> None:
    """Publica sincrónicamente (espera el ack) — el volumen esperado es bajo
    (una campaña activada = pocos eventos), no vale la pena el fire-and-forget."""
    await get_producer().send_and_wait(topic, value=value, key=key)
