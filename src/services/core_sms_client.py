from __future__ import annotations

import httpx

from config.settings import settings
from models.events import DeliveryResult

_HEADERS = {"X-Internal-Api-Key": settings.CORE_SMS_INTERNAL_API_KEY}


async def report_delivery_status(result: DeliveryResult) -> None:
    """Le pega a core_sms con el resultado de un envío (ver
    core_sms/src/routes/delivery_status.py). No atrapa errores a propósito:
    consumer_base.run_consumer_loop solo comitea el offset si el handler no
    lanza, así que si core_sms está caído el mensaje se reprocesa en el
    próximo poll en vez de perderse."""
    async with httpx.AsyncClient(base_url=settings.CORE_SMS_URL, timeout=10.0) as client:
        resp = await client.post(
            f"/campaigns/{result.campaign_id}/delivery-status",
            headers=_HEADERS,
            json=result.model_dump(mode="json"),
        )
    resp.raise_for_status()
