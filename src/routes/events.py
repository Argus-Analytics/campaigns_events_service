from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field

from config.settings import settings
from kafka.producer import publish
from kafka.topics import topic_for_provider
from models.events import CampaignEvent, MessageEvent, Provider

router = APIRouter(prefix="/events", tags=["events"])


# Secreto compartido simple, mismo patrón que
# billing_service/src/routes/internal.py — evita que cualquiera en la red
# local publique eventos falsos. El valor lo manda core_sms en
# events_client.py (ver core_sms/.env.example EVENTS_SERVICE_INTERNAL_API_KEY).
def _require_internal_key(x_internal_api_key: str | None = Header(default=None)) -> None:
    if x_internal_api_key != settings.INTERNAL_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid internal API key")


class SubCampaignIngest(BaseModel):
    key: str
    mensajes: list[MessageEvent] = Field(default_factory=list)
    estimated_reach: int = 0
    areas_geograficas: list[dict] = Field(default_factory=list)


class IngestCampaignRequest(BaseModel):
    campaign_id: str
    client_name: str
    pulpey_integration: bool = False
    subcampaigns: list[SubCampaignIngest]


def _providers_for(payload: IngestCampaignRequest) -> list[Provider]:
    """Asunción de negocio sin confirmar todavía: toda subcampaña se manda a
    AMBAS operadoras (Tigo + Claro cubren juntas la base de suscriptores de
    Guatemala), y a Pulpey solo si `pulpey_integration` está activo. El día
    que haya segmentación real por operadora (ver
    core_sms/src/services/reach_provider.py sobre por qué el alcance hoy no
    se calcula por operadora), esta lista debería volverse dinámica por
    subcampaña en vez de fija por campaña."""
    providers: list[Provider] = ["tigo_gt", "claro_gt"]
    if payload.pulpey_integration:
        providers.append("pulpey")
    return providers


@router.post("/campaigns", dependencies=[Depends(_require_internal_key)])
async def ingest_campaign(payload: IngestCampaignRequest) -> dict:
    providers = _providers_for(payload)
    published = 0
    for sub in payload.subcampaigns:
        for provider in providers:
            event = CampaignEvent(
                event_id=str(uuid.uuid4()),
                campaign_id=payload.campaign_id,
                client_name=payload.client_name,
                provider=provider,
                subcampaign_key=sub.key,
                mensajes=sub.mensajes,
                estimated_reach=sub.estimated_reach,
                areas_geograficas=sub.areas_geograficas,
                idempotency_key=f"{payload.campaign_id}:{sub.key}:{provider}",
            )
            await publish(topic_for_provider(provider), key=payload.campaign_id, value=event.model_dump(mode="json"))
            published += 1
    return {"published": published, "providers": providers}
