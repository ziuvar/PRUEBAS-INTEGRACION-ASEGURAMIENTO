# Guía de Evidencias

Este archivo te dice exactamente qué capturar para que el informe quede alineado con la rúbrica.

## 1. Evidencias mínimas del entorno

### RabbitMQ

Captura:

- `docker compose -f .\docker-compose.rabbitmq.yml ps`
- `docker compose -f .\docker-compose.rabbitmq.yml logs orders-api`
- `docker compose -f .\docker-compose.rabbitmq.yml logs notification-worker`
- consola `http://localhost:15672`
- cola `pedidos` visible

### Kafka

Captura:

- `docker compose -f .\docker-compose.kafka.yml ps`
- `docker compose -f .\docker-compose.kafka.yml logs orders-api`
- `docker compose -f .\docker-compose.kafka.yml logs inventory-consumer`
- `docker compose -f .\docker-compose.kafka.yml logs billing-consumer`
- `docker compose -f .\docker-compose.kafka.yml logs notification-consumer`
- consola `http://localhost:8081`
- topic `orders.events`
- consumer groups

## 2. Evidencias por caso

### CP-01 Creación de pedido vía API

Captura:

- petición en Postman o `curl`
- respuesta `202`
- cuerpo JSON con `status`, `message`, `orderId`

### CP-02 Envío de mensaje a RabbitMQ

Captura:

- logs de `orders-api`
- cola `pedidos` en RabbitMQ Management

### CP-03 Consumo de mensaje (RabbitMQ)

Captura:

- logs de `notification-worker`
- movimiento de contadores `Ready`, `Unacked`, `Total`

### CP-04 Publicación de evento en Kafka

Captura:

- logs de `orders-api`
- mensaje visible en `orders.events`

### CP-05 Consumo de evento (Kafka)

Captura:

- logs de cada consumidor
- offsets o particiones en Kafka UI

### CP-06 Trazabilidad del flujo

Captura:

- mismo `orderId` en API, broker y consumidor
- mismo `correlationId` en API y consumidores Kafka

### CP-07 Responsive

Captura:

- respuesta inmediata de la API
- logs del consumidor mostrando procesamiento después

### CP-08 Message-driven

Captura:

- ausencia de endpoints HTTP en consumidores
- evidencia del flujo solo a través del broker

### CP-09 Resilient

RabbitMQ:

- detener `notification-worker`
- enviar pedido
- capturar que el mensaje queda en `Ready`

Kafka:

- detener `notification-consumer`
- enviar pedido
- volver a levantar el consumidor
- capturar que el evento se procesa después

### CP-10 Elastic

RabbitMQ:

```powershell
docker compose -f .\docker-compose.rabbitmq.yml up -d --scale notification-worker=2
```

Captura:

- logs donde mensajes distintos son atendidos por instancias distintas

Kafka:

```powershell
docker compose -f .\docker-compose.kafka.yml up -d --scale notification-consumer=2
```

Captura:

- dos instancias del mismo grupo
- asignación de particiones o eventos por instancia

### CP-11 Desacoplamiento temporal

Captura:

- productor publica sin que consumidor esté activo
- consumo posterior cuando el servicio regresa

### CP-12 Recuperación

Captura:

- reinicio del consumidor
- reanudación de consumo
- logs verificables después del reinicio

## 3. Recomendación de orden para tomar capturas

1. Levantar stack.
2. Mostrar `docker compose ps`.
3. Mostrar consola del broker.
4. Enviar pedido.
5. Mostrar respuesta HTTP.
6. Mostrar logs del productor.
7. Mostrar logs del consumidor.
8. Ejecutar prueba con consumidor apagado.
9. Ejecutar prueba con múltiples consumidores.
10. Tomar conclusiones comparativas.
