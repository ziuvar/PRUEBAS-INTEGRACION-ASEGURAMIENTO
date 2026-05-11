# Taller de Pruebas de Integración

Solución completa del taller con dos stacks separados y simples de operar:

- `RabbitMQ`: arquitectura basada en mensajes.
- `Kafka`: arquitectura basada en eventos.

La implementación usa `Node.js + Express + Docker Compose` porque reduce al mínimo el código necesario y hace más clara la evidencia de integración.

## Estructura

```text
.
|-- docker-compose.rabbitmq.yml
|-- docker-compose.kafka.yml
|-- docs/
|   |-- EVIDENCE_GUIDE.md
|   `-- REPORT_TEMPLATE.md
|-- rabbitmq/
|   |-- notification-worker/
|   `-- orders-api/
`-- kafka/
    |-- consumer/
    `-- orders-api/
```

## Arquitectura elegida

### RabbitMQ

- `orders-api`: expone `POST /orders` y publica mensajes en la cola `pedidos`.
- `rabbitmq`: broker AMQP con consola de administración.
- `notification-worker`: consume mensajes desde la cola y confirma con `ack` manual.

### Kafka

- `orders-api`: expone `POST /orders` y publica el evento `OrderCreated` en `orders.events`.
- `kafka`: broker Kafka.
- `kafka-ui`: consola para observar topic, offsets y consumer groups.
- `inventory-consumer`, `billing-consumer`, `notification-consumer`: consumidores separados por responsabilidad, cada uno en un grupo distinto.

## Requisitos

- Docker Desktop con `docker compose`
- Puerto `3000` libre
- Puerto `15672` libre para RabbitMQ Management
- Puerto `8081` libre para Kafka UI
- Puerto `5672` libre para AMQP
- Puerto `29092` libre para acceso externo a Kafka

## Ejecución

### 1. RabbitMQ

```powershell
docker compose -f .\docker-compose.rabbitmq.yml up -d --build
docker compose -f .\docker-compose.rabbitmq.yml ps
```

Prueba rápida:

```powershell
curl.exe --% -s -X POST http://localhost:3000/orders -H "Content-Type: application/json" -d "{\"customerName\":\"Pedro\",\"product\":\"Libro de arquitectura\",\"quantity\":1}"
```

Consola RabbitMQ:

- URL: `http://localhost:15672`
- Usuario: `admin`
- Clave: `admin`

Logs útiles:

```powershell
docker compose -f .\docker-compose.rabbitmq.yml logs -f orders-api
docker compose -f .\docker-compose.rabbitmq.yml logs -f notification-worker
```

Pruebas reactivas importantes:

```powershell
docker compose -f .\docker-compose.rabbitmq.yml stop notification-worker
docker compose -f .\docker-compose.rabbitmq.yml up -d --scale notification-worker=2
```

Apagar stack:

```powershell
docker compose -f .\docker-compose.rabbitmq.yml down
```

### 2. Kafka

```powershell
docker compose -f .\docker-compose.kafka.yml up -d --build
docker compose -f .\docker-compose.kafka.yml ps
```

Prueba rápida:

```powershell
curl.exe --% -s -X POST http://localhost:3000/orders -H "Content-Type: application/json" -d "{\"customerName\":\"Pedro\",\"product\":\"Libro de arquitectura\",\"quantity\":1,\"unitPrice\":85000}"
```

Consola Kafka UI:

- URL: `http://localhost:8081`

Logs útiles:

```powershell
docker compose -f .\docker-compose.kafka.yml logs -f orders-api
docker compose -f .\docker-compose.kafka.yml logs -f inventory-consumer
docker compose -f .\docker-compose.kafka.yml logs -f billing-consumer
docker compose -f .\docker-compose.kafka.yml logs -f notification-consumer
```

Pruebas reactivas importantes:

```powershell
docker compose -f .\docker-compose.kafka.yml stop notification-consumer
docker compose -f .\docker-compose.kafka.yml up -d --scale notification-consumer=2
```

Apagar stack:

```powershell
docker compose -f .\docker-compose.kafka.yml down
```

## Evidencias e informe

La guía de capturas y el formato del informe están en:

- [docs/EVIDENCE_GUIDE.md](docs/EVIDENCE_GUIDE.md)
- [docs/REPORT_TEMPLATE.md](docs/REPORT_TEMPLATE.md)
- [docs/INTEGRATION_TEST_REPORT.md](docs/INTEGRATION_TEST_REPORT.md)
- [docs/INTEGRATION_TEST_REPORT.pdf](docs/INTEGRATION_TEST_REPORT.pdf)
- [docs/INTEGRATION_TEST_EVIDENCE_PACK.pdf](docs/INTEGRATION_TEST_EVIDENCE_PACK.pdf)

Tambien puedes regenerar evidencia tecnica real con:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\collect-evidence.ps1
```

Esto crea un set reproducible en `docs/evidence/latest/` con respuestas HTTP, `docker compose ps`, logs, estados de cola y consumer groups.

## Notas importantes para el taller

- Los stacks están pensados para ejecutarse por separado, no al mismo tiempo.
- En ambos casos la API responde `202 Accepted` sin esperar al consumidor.
- No existe comunicación HTTP entre productor y consumidores.
- En Kafka el topic se crea con `2` particiones para facilitar evidencia de offsets y escalamiento.
