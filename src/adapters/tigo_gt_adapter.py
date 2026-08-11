from __future__ import annotations

from datetime import datetime, timezone

from models.events import CampaignEvent, DeliveryResult

# Sin contrato/acceso a la API de Tigo Guatemala todavía — este adapter solo
# marca el evento como pending_integration. Reemplazar el cuerpo de send()
# por la llamada real el día que exista API (mismo seam que
# core_sms/src/services/reach_provider.py describe del lado del alcance).


async def send(event: CampaignEvent) -> DeliveryResult:
    return DeliveryResult(
        event_id=event.event_id,
        campaign_id=event.campaign_id,
        subcampaign_key=event.subcampaign_key,
        provider=event.provider,
        status="pending_integration",
        detail="Sin integración real con Tigo Guatemala todavía",
        processed_at=datetime.now(timezone.utc),
    )
