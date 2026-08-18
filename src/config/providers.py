from __future__ import annotations

from typing import Literal, TypedDict

from models.events import Provider

ProviderMode = Literal["api", "manual"]


class ProviderInfo(TypedDict):
    # ISO-2 en minúscula del país al que sirve este proveedor, o None para
    # uno país-agnóstico (hoy solo pulpey). Ver routes/events.py::_providers_for
    # — sin esto, una campaña de Honduras también se mandaría a tigo_gt/claro_gt.
    country: str | None
    mode: ProviderMode
    # Días hábiles que el proveedor necesita entre que se le entrega la
    # campaña y el envío efectivo. Rango porque en el caso manual varía según
    # carga de trabajo del proveedor — core_sms usa el máximo (peor caso) para
    # decidir si todavía hay margen para activar.
    lead_business_days_min: int
    lead_business_days_max: int
    # Si el proveedor recibe la campaña por default cuando el cliente no
    # especifica nada. Hoy: Tigo/Claro sí, Pulpey no (opt-in vía
    # Campaign.pulpey_integration, ver core_sms/src/models/campaign.py).
    default_enabled: bool
    # Si el proveedor puede enviar SMS en absoluto en su país — ver
    # core_sms/src/services/provider_registry.py::ProviderInfo.available
    # (misma definición). Hoy: Tigo Costa Rica no tiene ninguna vía de envío
    # todavía, ni siquiera manual — routes/events.py::_providers_for lo
    # excluye siempre, sin importar disabled_providers.
    available: bool


# Fuente de verdad de a qué se compromete cada proveedor — core_sms mantiene
# una copia de estos mismos valores en services/provider_registry.py para no
# depender de una llamada de red en el camino crítico de activar una campaña
# (ver ese archivo). Si se agrega/cambia un proveedor acá, hay que
# actualizarlo ahí también.
PROVIDERS: dict[Provider, ProviderInfo] = {
    "claro_gt": {"country": "gt", "mode": "manual", "lead_business_days_min": 10, "lead_business_days_max": 20, "default_enabled": True, "available": True},
    "tigo_gt":  {"country": "gt", "mode": "api",    "lead_business_days_min": 0,  "lead_business_days_max": 1,  "default_enabled": True, "available": True},
    "claro_hn": {"country": "hn", "mode": "manual", "lead_business_days_min": 10, "lead_business_days_max": 20, "default_enabled": True, "available": True},
    "tigo_hn":  {"country": "hn", "mode": "api",    "lead_business_days_min": 0,  "lead_business_days_max": 1,  "default_enabled": True, "available": True},
    "claro_sv": {"country": "sv", "mode": "manual", "lead_business_days_min": 10, "lead_business_days_max": 20, "default_enabled": True, "available": True},
    "tigo_sv":  {"country": "sv", "mode": "api",    "lead_business_days_min": 0,  "lead_business_days_max": 1,  "default_enabled": True, "available": True},
    "claro_ni": {"country": "ni", "mode": "manual", "lead_business_days_min": 10, "lead_business_days_max": 20, "default_enabled": True, "available": True},
    "tigo_ni":  {"country": "ni", "mode": "api",    "lead_business_days_min": 0,  "lead_business_days_max": 1,  "default_enabled": True, "available": True},
    "claro_cr": {"country": "cr", "mode": "manual", "lead_business_days_min": 10, "lead_business_days_max": 20, "default_enabled": True, "available": True},
    "tigo_cr":  {"country": "cr", "mode": "api",    "lead_business_days_min": 0,  "lead_business_days_max": 1,  "default_enabled": True, "available": False},
    "pulpey":   {"country": None, "mode": "api",    "lead_business_days_min": 0,  "lead_business_days_max": 1,  "default_enabled": False, "available": True},
}
