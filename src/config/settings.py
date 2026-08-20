from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    APP_TITLE: str = "campaigns_events_service"

    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    INTERNAL_API_KEY: str = "dev-internal-key"

    # Ver rate_limit.py — este servicio no lo llama el front (solo core_sms,
    # server-a-servidor), así que es más una red de seguridad por defecto que
    # una protección activa: la única ruta real (/events/campaigns) está exenta.
    RATE_LIMIT_DEFAULT: str = "60/minute"

    CORE_SMS_URL: str = "http://localhost:8300"
    CORE_SMS_INTERNAL_API_KEY: str = "dev-internal-key"


settings = Settings()
