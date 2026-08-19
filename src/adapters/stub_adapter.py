from __future__ import annotations

from datetime import datetime, timezone
from typing import Awaitable, Callable

from models.events import CampaignEvent, DeliveryResult

# Fábrica de adapters sin integración real todavía — mismo comportamiento que
# tigo_gt_adapter.py/claro_gt_adapter.py (entrega simulada al 100% del
# alcance estimado), pero parametrizado por operadora en vez de duplicar el
# mismo cuerpo de send() por cada país (ver workers/provider_worker.py).
# Reemplazar por un adapter propio (como los de GT) el día que exista
# contrato/acceso real con esa operadora en ese país.


def make_stub_adapter(display_name: str) -> Callable[[CampaignEvent], Awaitable[DeliveryResult]]:
    async def send(event: CampaignEvent) -> DeliveryResult:
        return DeliveryResult(
            event_id=event.event_id,
            campaign_id=event.campaign_id,
            subcampaign_key=event.subcampaign_key,
            provider=event.provider,
            status="delivered",
            delivered_count=event.estimated_reach,
            detail=f"Simulado (sin integración real con {display_name} todavía)",
            processed_at=datetime.now(timezone.utc),
        )

    return send
