from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

# "pulpey" no está confirmado como integración real todavía (ver
# adapters/pulpey_adapter.py) — vive en el mismo Literal que tigo_gt/claro_gt
# porque comparte el mismo pipeline de eventos, no porque ya tenga API real.
# Los proveedores de HN/SV/NI/CR tampoco tienen integración real todavía (ver
# adapters/stub_adapter.py) — mismo caso que tigo_gt/claro_gt cuando se
# agregaron, listos para conectar la API real cuando haya contrato/acceso.
Provider = Literal[
    "pulpey",
    "tigo_gt", "claro_gt",
    "tigo_hn", "claro_hn",
    "tigo_sv", "claro_sv",
    "tigo_ni", "claro_ni",
    "tigo_cr", "claro_cr",
]
DeliveryStatus = Literal["sent", "pending_integration", "failed"]


class MessageEvent(BaseModel):
    sms_message: str
    horario_envio: dict = Field(default_factory=dict)


class CampaignEvent(BaseModel):
    """Un mensaje del topic campaigns.<provider> — una unidad = una
    subcampaña dirigida a un proveedor puntual (ver routes/events.py sobre
    cómo se decide a qué provider(s) se manda cada subcampaña)."""

    event_id: str
    campaign_id: str
    client_name: str
    provider: Provider
    subcampaign_key: str
    mensajes: list[MessageEvent] = Field(default_factory=list)
    estimated_reach: int = 0
    areas_geograficas: list[dict] = Field(default_factory=list)
    idempotency_key: str


class DeliveryResult(BaseModel):
    """Un mensaje del topic campaigns.delivery-status — lo produce cada
    provider worker después de llamar a su adapter, lo consume
    workers/delivery_status_worker.py para reportarle a core_sms."""

    event_id: str
    campaign_id: str
    subcampaign_key: str
    provider: Provider
    status: DeliveryStatus
    detail: str = ""
    processed_at: datetime
