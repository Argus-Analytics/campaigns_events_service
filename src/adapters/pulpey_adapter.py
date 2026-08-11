from __future__ import annotations

from datetime import datetime, timezone

from models.events import CampaignEvent, DeliveryResult

# core_sms ya tiene datos de promociones Pulpey (pulpey_promotions_store.py),
# pero eso es un dominio distinto (promos internas) del canal de envío de la
# campaña en sí — no hay confirmado un contrato/API para publicar campañas en
# la app Pulpey. Stub hasta que exista, mismo patrón que tigo_gt/claro_gt.


async def send(event: CampaignEvent) -> DeliveryResult:
    return DeliveryResult(
        event_id=event.event_id,
        campaign_id=event.campaign_id,
        subcampaign_key=event.subcampaign_key,
        provider=event.provider,
        status="pending_integration",
        detail="Sin integración real con Pulpey todavía",
        processed_at=datetime.now(timezone.utc),
    )
