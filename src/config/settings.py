from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    APP_TITLE: str = "campaigns_events_service"

    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    INTERNAL_API_KEY: str = "dev-internal-key"

    CORE_SMS_URL: str = "http://localhost:8300"
    CORE_SMS_INTERNAL_API_KEY: str = "dev-internal-key"


settings = Settings()
