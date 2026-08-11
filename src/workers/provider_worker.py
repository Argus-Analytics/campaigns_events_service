from __future__ import annotations

import asyncio
import os

from adapters import claro_gt_adapter, pulpey_adapter, tigo_gt_adapter
from kafka.consumer_base import run_consumer_loop
from kafka.producer import publish, start_producer, stop_producer
from kafka.topics import TOPIC_DELIVERY_STATUS, topic_for_provider
from models.events import CampaignEvent, Provider

_ADAPTERS = {
    "pulpey": pulpey_adapter.send,
    "tigo_gt": tigo_gt_adapter.send,
    "claro_gt": claro_gt_adapter.send,
}


async def _handle(provider: Provider, raw: dict) -> None:
    event = CampaignEvent.model_validate(raw)
    result = await _ADAPTERS[provider](event)
    await publish(TOPIC_DELIVERY_STATUS, key=result.campaign_id, value=result.model_dump(mode="json"))


async def main() -> None:
    provider = os.environ["PROVIDER"]  # tigo_gt | claro_gt | pulpey — ver docker-compose.yml
    if provider not in _ADAPTERS:
        raise ValueError(f"PROVIDER inválido: {provider!r} — debe ser uno de {list(_ADAPTERS)}")
    topic = topic_for_provider(provider)  # type: ignore[arg-type]
    await start_producer()
    try:
        await run_consumer_loop(
            topic,
            group_id=f"provider-worker-{provider}",
            handler=lambda raw: _handle(provider, raw),  # type: ignore[arg-type]
        )
    finally:
        await stop_producer()


if __name__ == "__main__":
    asyncio.run(main())
