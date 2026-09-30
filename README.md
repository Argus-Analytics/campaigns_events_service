# campaigns_events_service

Servicio que recibe campañas (vía HTTP, desde `core_sms`) y las publica como
eventos en Redpanda, uno por proveedor de envío. Todo lo relacionado con el
broker (producer, consumers) vive acá — `core_sms` nunca importa un cliente
Kafka/Redpanda, solo hace HTTP hacia este servicio y expone un endpoint HTTP
para recibir el resultado de vuelta.

```
core_sms --HTTP--> [ingestion-api] --Redpanda--> [worker-tigo-gt]    --Redpanda--> [worker-delivery-status] --HTTP--> core_sms
                                             \--> [worker-claro-gt]  --/
                                             \--> [worker-pulpey]    --/
```

## Topics

| Topic | Quién publica | Quién consume |
|---|---|---|
| `campaigns.pulpey` | ingestion-api | worker-pulpey |
| `campaigns.tigo.gt` | ingestion-api | worker-tigo-gt |
| `campaigns.claro.gt` | ingestion-api | worker-claro-gt |
| `campaigns.delivery-status` | los 3 workers de proveedor | worker-delivery-status |

Key de partición en los 4 topics = `campaign_id` (orden garantizado por
campaña).

## Estado de las integraciones (2026-08-10)

**Ninguno de los 3 proveedores tiene API real conectada todavía.** Los tres
adapters (`src/adapters/{pulpey,tigo_gt,claro_gt}_adapter.py`) son stubs: al
recibir un evento, devuelven `status="pending_integration"` sin llamar a
ningún servicio externo. Cuando exista un contrato/acceso real con alguno,
el cambio es puntual: reemplazar el cuerpo de `send()` en ese adapter — el
resto del pipeline (ingestión, topics, delivery-status, callback a
`core_sms`) no debería necesitar cambios.

## Asunción de negocio sin confirmar

`routes/events.py::_providers_for()` hoy manda **toda subcampaña a Tigo Y
Claro** (asumiendo que juntas cubren la base de suscriptores de Guatemala), y
a Pulpey solo si la campaña tiene `pulpey_integration=true`. Esto no está
confirmado con el negocio — revisar antes de conectar APIs reales, porque el
día que haya segmentación real por operadora esto debería decidirse por
subcampaña, no fijo por campaña completa (ver
`core_sms/src/services/reach_provider.py` sobre por qué el alcance hoy no se
calcula por operadora).

## Cómo correrlo

```bash
cp .env.example .env
docker compose up -d --build
```

- Ingestion API: `http://localhost:8302` (`GET /health`)
- Redpanda Console: `http://localhost:8090`
- Broker Redpanda (para debug con clientes externos, protocolo Kafka): `localhost:19093`

`INTERNAL_API_KEY` acá debe ser igual a `EVENTS_SERVICE_INTERNAL_API_KEY` en
el `.env` de `core_sms`. `CORE_SMS_INTERNAL_API_KEY` acá debe ser igual a
`INTERNAL_API_KEY` en el `.env` de `core_sms`. Mismo patrón de secreto
compartido simple que ya usa `billing_service` (ver su
`src/routes/internal.py`) — no hay auth real de servicio-a-servicio todavía
en el ecosistema.

## Integración con core_sms

- `core_sms` llama `POST /events/campaigns` al activar una campaña (ver
  `core_sms/src/services/events_client.py`, llamado desde
  `routes/campaigns.py::activate_campaign`). Si este servicio está caído, la
  activación de la campaña **no falla** — solo se loggea el error, porque los
  créditos ya se reservaron y el estado ya cambió a `active` del lado de
  `core_sms`.
- `worker-delivery-status` llama `POST /campaigns/{id}/delivery-status` en
  `core_sms` (ver `core_sms/src/routes/delivery_status.py`) cada vez que un
  adapter de proveedor procesa un evento. El resultado se guarda en
  `Campaign.delivery_status`, keyed por `"{subcampaign_key}:{provider}"`.

## Por qué Redpanda (y no Apache Kafka)

Se migró de Apache Kafka real (modo KRaft) a Redpanda en un solo nodo
(`--mode dev-container`) porque el resto de la infraestructura ya corre
Redpanda — la decisión previa de "usar el mismo binario que un cluster
compartido" ya no aplica porque ese cluster compartido es Redpanda, no Kafka.
Como Redpanda habla el protocolo Kafka, `aiokafka` (producer.py,
consumer_base.py) no cambió nada de código; el único delta es
infraestructura: `docker-compose.yml` (imagen, healthcheck, creación de
topics con `rpk` en vez de `kafka-topics.sh`) y Redpanda Console en vez de
`kafka-ui` para inspección visual.
