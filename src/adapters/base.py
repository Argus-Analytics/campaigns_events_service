from __future__ import annotations

from typing import Protocol

from models.events import CampaignEvent, DeliveryResult


class ProviderAdapter(Protocol):
    async def send(self, event: CampaignEvent) -> DeliveryResult: ...
