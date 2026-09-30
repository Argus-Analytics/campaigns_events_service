"""routes/events.py::_providers_for — con proveedores de varios países en la
tabla (ver config/providers.py::PROVIDERS), el filtro por país es lo que
evita que una campaña de un país se mande también a los proveedores de otro."""

from __future__ import annotations

from routes.events import IngestCampaignRequest, _providers_for


def _payload(**overrides) -> IngestCampaignRequest:
    base = dict(campaign_id="camp-1", client_name="Cliente demo", subcampaigns=[])
    base.update(overrides)
    return IngestCampaignRequest(**base)


def test_providers_for_scopes_to_campaign_country():
    assert set(_providers_for(_payload(country="hn"))) == {"tigo_hn", "claro_hn"}


def test_providers_for_does_not_leak_other_countries_providers():
    providers = _providers_for(_payload(country="gt"))
    assert "tigo_hn" not in providers
    assert "claro_hn" not in providers


def test_providers_for_defaults_to_gt_when_country_omitted():
    assert set(_providers_for(_payload())) == {"tigo_gt", "claro_gt"}


def test_providers_for_respects_disabled_providers():
    providers = _providers_for(_payload(country="sv", disabled_providers=["claro_sv"]))
    assert providers == ["tigo_sv"]


def test_providers_for_adds_pulpey_when_opted_in_regardless_of_country():
    providers = _providers_for(_payload(country="sv", pulpey_integration=True))
    assert set(providers) == {"tigo_sv", "claro_sv", "pulpey"}


def test_providers_for_country_is_case_insensitive():
    assert set(_providers_for(_payload(country="HN"))) == {"tigo_hn", "claro_hn"}


def test_providers_for_excludes_unavailable_provider_even_if_not_disabled():
    """Tigo Costa Rica no tiene ninguna vía de envío todavía — nunca debería
    aparecer, ni siquiera "activado" por default."""
    assert _providers_for(_payload(country="cr")) == ["claro_cr"]


async def test_ingest_forwards_the_base_of_each_subcampaign_to_every_provider(monkeypatch):
    """La base (prearmada / a medida / nueva) viaja en cada evento — sin esto
    la telco recibiría el mensaje sin saber a qué segmento va dirigido."""
    from routes import events

    published: list[dict] = []

    async def _publish(topic, key, value):
        published.append(value)

    monkeypatch.setattr(events, "publish", _publish)
    base = {"tipo": "nueva", "key": "custom-abc", "demografia": {"genero": "ambos"}}
    payload = _payload(subcampaigns=[{"key": "s1", "base": base}])

    await events.ingest_campaign(payload)

    assert len(published) == 2  # tigo_gt + claro_gt
    assert all(e["base"] == base for e in published)
