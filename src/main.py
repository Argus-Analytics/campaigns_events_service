from contextlib import asynccontextmanager

from fastapi import FastAPI

from config.settings import settings
from kafka.producer import start_producer, stop_producer
from routes.events import router as events_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    await start_producer()
    yield
    await stop_producer()


app = FastAPI(title=settings.APP_TITLE, version="0.1.0", lifespan=lifespan)

app.include_router(events_router)


@app.get("/health")
async def health():
    return {"status": "ok"}
