from __future__ import annotations

import json
import logging
from typing import Awaitable, Callable

from aiokafka import AIOKafkaConsumer

from config.settings import settings

logger = logging.getLogger(__name__)


async def run_consumer_loop(topic: str, group_id: str, handler: Callable[[dict], Awaitable[None]]) -> None:
    """Loop infinito: consume `topic` en `group_id`, llama `handler(value)` por
    mensaje y hace commit manual solo si el handler no lanzó excepción — un
    error deja el mensaje sin commitear para que se reprocese (at-least-once,
    los handlers deben ser idempotentes vía `idempotency_key`/`event_id`)."""
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        async for msg in consumer:
            try:
                await handler(msg.value)
            except Exception:
                logger.exception("Error procesando mensaje de %s (offset=%s) — no se hace commit", topic, msg.offset)
                continue
            await consumer.commit()
    finally:
        await consumer.stop()
