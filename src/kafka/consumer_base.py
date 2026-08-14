from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable

from aiokafka import AIOKafkaConsumer

from config.settings import settings
from kafka.producer import publish
from kafka.topics import TOPIC_DEAD_LETTER

logger = logging.getLogger(__name__)

# Reintentos EN PROCESO para el mismo mensaje antes de tratarlo como poison.
# No hay estado persistido entre redelivers (el consumer arranca siempre
# desde el último offset comiteado), así que esto solo cubre blips
# transitorios dentro de la misma pasada — mismo criterio que
# core_sms/services/{billing_client,events_client}.py. Si el handler sigue
# fallando después de esto, el mensaje se manda a TOPIC_DEAD_LETTER y se
# comitea el offset original de todos modos: antes un mensaje poison
# bloqueaba el topic/partición para siempre (reproceso infinito).
_MAX_ATTEMPTS = 3
_RETRY_DELAY_SECONDS = 0.5


async def _process_with_retry(
    handler: Callable[[dict], Awaitable[None]], value: dict, topic: str, offset: int,
) -> Exception | None:
    """Intenta `handler(value)` hasta _MAX_ATTEMPTS veces. Devuelve la última
    excepción si se agotaron los intentos, o None si tuvo éxito en alguno."""
    last_error: Exception | None = None
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            await handler(value)
            return None
        except Exception as exc:  # noqa: BLE001 — cualquier excepción del handler es candidata a reintento/DLQ
            last_error = exc
            if attempt < _MAX_ATTEMPTS:
                logger.warning(
                    "Intento %d/%d fallido procesando %s (offset=%s) — reintentando",
                    attempt, _MAX_ATTEMPTS, topic, offset,
                )
                await asyncio.sleep(_RETRY_DELAY_SECONDS)
    return last_error


async def _send_to_dead_letter(topic: str, group_id: str, msg, error: Exception) -> None:
    key = msg.key.decode("utf-8") if isinstance(msg.key, bytes) else msg.key
    logger.error(
        "Mensaje poison en %s (offset=%s) tras %d intentos — enviando a %s",
        topic, msg.offset, _MAX_ATTEMPTS, TOPIC_DEAD_LETTER, exc_info=error,
    )
    await publish(TOPIC_DEAD_LETTER, key=key, value={
        "source_topic": topic,
        "source_partition": msg.partition,
        "source_offset": msg.offset,
        "group_id": group_id,
        "error": str(error),
        "value": msg.value,
    })


async def run_consumer_loop(topic: str, group_id: str, handler: Callable[[dict], Awaitable[None]]) -> None:
    """Loop infinito: consume `topic` en `group_id`, llama `handler(value)` por
    mensaje, reintentando hasta _MAX_ATTEMPTS veces ante una excepción. Si
    sigue fallando, el mensaje se publica en TOPIC_DEAD_LETTER (con el error y
    de dónde vino) y se comitea el offset original de todos modos, para no
    bloquear el resto del topic/partición — antes esto no tenía tope y un
    mensaje poison colgaba el consumo para siempre. At-least-once solo dentro
    de esos reintentos: los handlers deben seguir siendo idempotentes vía
    idempotency_key/event_id."""
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
            error = await _process_with_retry(handler, msg.value, topic, msg.offset)
            if error is not None:
                await _send_to_dead_letter(topic, group_id, msg, error)
            await consumer.commit()
    finally:
        await consumer.stop()
