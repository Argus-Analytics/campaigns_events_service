from __future__ import annotations

from datetime import datetime, timezone

from models.events import CampaignEvent, DeliveryResult

# Sin contrato/acceso a la API de Tigo Guatemala todavía. Simula una entrega
# confirmada al 100% del alcance estimado (delivered_count=estimated_reach)
# para poder ejercer el pipeline de reconciliación de créditos de punta a
# punta (ver core_sms/src/routes/delivery_status.py) mientras no hay
# proveedor real — reemplazar por la llamada real y su conteo real de
# entregas el día que exista API (mismo seam que
# core_sms/src/services/reach_provider.py describe del lado del alcance).


async def send(event: CampaignEvent) -> DeliveryResult:
    return DeliveryResult(
        event_id=event.event_id,
        campaign_id=event.campaign_id,
        subcampaign_key=event.subcampaign_key,
        provider=event.provider,
        status="delivered",
        delivered_count=event.estimated_reach,
        detail="Simulado (sin integración real con Tigo Guatemala todavía)",
        processed_at=datetime.now(timezone.utc),
    )
