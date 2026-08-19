from __future__ import annotations

from datetime import datetime, timezone

from models.events import CampaignEvent, DeliveryResult

# Sin contrato/acceso a la API de Claro Guatemala todavía — mismo caso que
# tigo_gt_adapter.py, incluida la simulación de entrega confirmada.


async def send(event: CampaignEvent) -> DeliveryResult:
    return DeliveryResult(
        event_id=event.event_id,
        campaign_id=event.campaign_id,
        subcampaign_key=event.subcampaign_key,
        provider=event.provider,
        status="delivered",
        delivered_count=event.estimated_reach,
        detail="Simulado (sin integración real con Claro Guatemala todavía)",
        processed_at=datetime.now(timezone.utc),
    )
