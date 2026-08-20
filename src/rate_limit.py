from __future__ import annotations

from slowapi import Limiter
from slowapi.util import get_remote_address

from config.settings import settings

# Storage en memoria del proceso — correcto para una sola instancia. Si este
# servicio corre con más de un worker/réplica, cada uno cuenta por su cuenta
# — para eso hace falta un backend compartido (Redis) vía
# Limiter(storage_uri=...).
limiter = Limiter(key_func=get_remote_address, default_limits=[settings.RATE_LIMIT_DEFAULT])
