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
DeliveryStatus = Literal["sent", "delivered", "pending_integration", "failed"]


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
    # Base que la telco debe usar (prearmada / a medida / nueva) — ver
    # core_sms/src/services/bases.py::telco_base_payload. Si tipo='nueva', la
    # telco crea la base a nivel país y envía en `areas_geograficas`.
    base: dict | None = None
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
    # Cuántos SMS confirmó este evento como entregados — solo > 0 cuando
    # status="delivered" (billing_service cobra créditos reales sobre esto,
    # nunca sobre "sent"/pending_integration — ver core_sms/src/routes/
    # delivery_status.py). Simulado hoy con estimated_reach mientras los
    # adapters sean stubs (ver adapters/*.py); reemplazar por el conteo real
    # que devuelva cada proveedor el día que haya integración.
    delivered_count: int = 0
    detail: str = ""
    processed_at: datetime
