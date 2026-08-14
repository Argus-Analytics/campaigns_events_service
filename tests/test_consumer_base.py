"""kafka/consumer_base.py — retry + dead-letter queue. Antes de este fix, un
mensaje que siempre fallaba (payload corrupto, adapter que lanza excepción)
nunca comiteaba el offset y el consumer reintentaba ese mismo mensaje para
siempre, bloqueando el topic/partición (ver ARCHITECTURE.md / auditoría de
error handling). Estos tests fijan que ahora, tras agotar los reintentos, el
mensaje se manda a TOPIC_DEAD_LETTER y el offset original SÍ se comitea."""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock

from kafka import consumer_base
from kafka.topics import TOPIC_DEAD_LETTER


@dataclass
class _FakeMessage:
    value: dict
    offset: int = 0
    partition: int = 0
    key: bytes | None = None


class _FakeConsumer:
    """Sustituye a AIOKafkaConsumer — sin red real, ver monkeypatch de
    consumer_base.AIOKafkaConsumer en cada test (import local, no alcanza con
    parchear aiokafka.AIOKafkaConsumer directamente)."""

    def __init__(self, messages: list[_FakeMessage]):
        self._messages = messages
        self.commits = 0
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def commit(self) -> None:
        self.commits += 1

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        for msg in self._messages:
            yield msg


# ── _process_with_retry — lógica pura de reintento, sin Kafka ──────────────


async def test_process_with_retry_succeeds_without_retrying():
    calls = []

    async def handler(value):
        calls.append(value)

    error = await consumer_base._process_with_retry(handler, {"x": 1}, "topic", 0)

    assert error is None
    assert calls == [{"x": 1}]


async def test_process_with_retry_retries_transient_failure_and_succeeds(monkeypatch):
    monkeypatch.setattr(consumer_base, "_RETRY_DELAY_SECONDS", 0)
    attempts = {"count": 0}

    async def handler(value):
        attempts["count"] += 1
        if attempts["count"] < consumer_base._MAX_ATTEMPTS:
            raise ValueError("transient")

    error = await consumer_base._process_with_retry(handler, {"x": 1}, "topic", 0)

    assert error is None
    assert attempts["count"] == consumer_base._MAX_ATTEMPTS


async def test_process_with_retry_gives_up_and_returns_last_error(monkeypatch):
    monkeypatch.setattr(consumer_base, "_RETRY_DELAY_SECONDS", 0)
    attempts = {"count": 0}

    async def handler(value):
        attempts["count"] += 1
        raise ValueError(f"poison {attempts['count']}")

    error = await consumer_base._process_with_retry(handler, {"x": 1}, "topic", 0)

    assert attempts["count"] == consumer_base._MAX_ATTEMPTS
    assert isinstance(error, ValueError)
    assert str(error) == f"poison {consumer_base._MAX_ATTEMPTS}"


# ── run_consumer_loop — comportamiento end-to-end con un consumer fake ─────


async def test_run_consumer_loop_commits_good_messages_without_touching_dlq(monkeypatch):
    monkeypatch.setattr(consumer_base, "_RETRY_DELAY_SECONDS", 0)
    fake_consumer = _FakeConsumer([_FakeMessage(value={"ok": True}, offset=1)])
    monkeypatch.setattr(consumer_base, "AIOKafkaConsumer", lambda *a, **kw: fake_consumer)
    publish_mock = AsyncMock()
    monkeypatch.setattr(consumer_base, "publish", publish_mock)

    async def handler(value):
        pass

    await consumer_base.run_consumer_loop("topic", "group", handler)

    assert fake_consumer.commits == 1
    publish_mock.assert_not_awaited()
    assert fake_consumer.started and fake_consumer.stopped


async def test_run_consumer_loop_sends_poison_message_to_dead_letter_and_still_commits(monkeypatch):
    monkeypatch.setattr(consumer_base, "_RETRY_DELAY_SECONDS", 0)
    fake_consumer = _FakeConsumer([_FakeMessage(value={"bad": True}, offset=7, partition=2, key=b"camp-1")])
    monkeypatch.setattr(consumer_base, "AIOKafkaConsumer", lambda *a, **kw: fake_consumer)
    publish_mock = AsyncMock()
    monkeypatch.setattr(consumer_base, "publish", publish_mock)

    async def handler(value):
        raise ValueError("payload corrupto")

    await consumer_base.run_consumer_loop("campaigns.tigo.gt", "provider-worker-tigo_gt", handler)

    # El offset original SÍ se comitea — antes quedaba colgado para siempre.
    assert fake_consumer.commits == 1
    publish_mock.assert_awaited_once()
    args, kwargs = publish_mock.call_args
    assert args[0] == TOPIC_DEAD_LETTER
    assert kwargs["key"] == "camp-1"
    envelope = kwargs["value"]
    assert envelope["source_topic"] == "campaigns.tigo.gt"
    assert envelope["source_partition"] == 2
    assert envelope["source_offset"] == 7
    assert envelope["group_id"] == "provider-worker-tigo_gt"
    assert envelope["value"] == {"bad": True}
    assert "payload corrupto" in envelope["error"]


async def test_run_consumer_loop_keeps_processing_later_messages_after_a_poison_one(monkeypatch):
    """Un mensaje poison no debe bloquear los mensajes siguientes del mismo
    topic/partición — antes sí lo hacía (nunca comiteaba y el consumer
    reintentaba ese mismo offset para siempre)."""
    monkeypatch.setattr(consumer_base, "_RETRY_DELAY_SECONDS", 0)
    fake_consumer = _FakeConsumer([
        _FakeMessage(value={"id": 1}, offset=1),
        _FakeMessage(value={"id": 2}, offset=2),
    ])
    monkeypatch.setattr(consumer_base, "AIOKafkaConsumer", lambda *a, **kw: fake_consumer)
    monkeypatch.setattr(consumer_base, "publish", AsyncMock())
    processed = []

    async def handler(value):
        if value["id"] == 1:
            raise ValueError("poison")
        processed.append(value)

    await consumer_base.run_consumer_loop("topic", "group", handler)

    assert processed == [{"id": 2}]
    assert fake_consumer.commits == 2
