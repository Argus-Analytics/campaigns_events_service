from __future__ import annotations

import asyncio

from kafka.consumer_base import run_consumer_loop
from kafka.producer import start_producer, stop_producer
from kafka.topics import TOPIC_DELIVERY_STATUS
from models.events import DeliveryResult
from services import core_sms_client


async def _handle(raw: dict) -> None:
    result = DeliveryResult.model_validate(raw)
    await core_sms_client.report_delivery_status(result)


async def main() -> None:
    # run_consumer_loop publica al topic dead-letter (ver kafka/consumer_base.py)
    # si un mensaje agota los reintentos — necesita el producer arrancado,
    # aunque este worker no publique nada más por su cuenta.
    await start_producer()
    try:
        await run_consumer_loop(TOPIC_DELIVERY_STATUS, group_id="delivery-status-worker", handler=_handle)
    finally:
        await stop_producer()


if __name__ == "__main__":
    asyncio.run(main())
