# Informe de Pruebas de Integración

## 1. Datos generales

- Asignatura: Aseguramiento de la Calidad
- Taller: Pruebas de Integración en Arquitecturas Basadas en Mensajes y Eventos
- Integrantes:
- Fecha:

## 2. Arquitectura implementada

### 2.1 RabbitMQ

- Productor: `orders-api`
- Broker: `rabbitmq`
- Consumidor: `notification-worker`
- Cola: `pedidos`

### 2.2 Kafka

- Productor: `orders-api`
- Broker: `kafka`
- Consola: `kafka-ui`
- Topic: `orders.events`
- Consumidores:
  - `inventory-consumer`
  - `billing-consumer`
  - `notification-consumer`

## 3. Evidencia del entorno

### RabbitMQ

- Captura de `docker compose ps`
- Captura de RabbitMQ Management
- Captura de logs de arranque

### Kafka

- Captura de `docker compose ps`
- Captura de Kafka UI
- Captura de logs de arranque

## 4. Casos de prueba

### CP-01 Creación de pedido vía API

- Arquitectura: RabbitMQ / Kafka
- Objetivo: Validar que el endpoint `POST /orders` recibe la solicitud y responde correctamente.
- Precondiciones: Stack levantado y API disponible en `http://localhost:3000`.
- Datos de entrada:

```json
{
  "customerName": "Pedro",
  "product": "Libro de arquitectura",
  "quantity": 1
}
```

- Pasos ejecutados:
  1. Enviar petición `POST /orders`.
  2. Verificar código de respuesta y cuerpo JSON.
- Resultado esperado: La API responde `202 Accepted` y entrega `status`, `message` y `orderId`.
- Resultado obtenido:
- Evidencia:
- Estado: Cumple / No cumple
- Análisis técnico:

### CP-02 Envío de mensaje a RabbitMQ

- Arquitectura: RabbitMQ
- Objetivo: Verificar que el pedido genera un mensaje en la cola `pedidos`.
- Precondiciones: `rabbitmq` y `orders-api` en ejecución.
- Datos de entrada: Pedido enviado a `POST /orders`.
- Pasos ejecutados:
  1. Enviar pedido a la API.
  2. Revisar logs de `orders-api`.
  3. Revisar la cola `pedidos` en RabbitMQ Management.
- Resultado esperado: El mensaje queda publicado en la cola.
- Resultado obtenido:
- Evidencia:
- Estado: Cumple / No cumple
- Análisis técnico:

### CP-03 Consumo de mensaje (RabbitMQ)

- Arquitectura: RabbitMQ
- Objetivo: Validar que `notification-worker` consume y procesa el mensaje.
- Precondiciones: `notification-worker` en ejecución.
- Datos de entrada: Pedido enviado a `POST /orders`.
- Pasos ejecutados:
  1. Enviar pedido.
  2. Revisar logs del worker.
  3. Verificar confirmación de consumo.
- Resultado esperado: El worker muestra recepción, procesamiento y `ack` del mensaje.
- Resultado obtenido:
- Evidencia:
- Estado: Cumple / No cumple
- Análisis técnico:

### CP-04 Publicación de evento en Kafka

- Arquitectura: Kafka
- Objetivo: Verificar que la API publica el evento `OrderCreated` en el topic `orders.events`.
- Precondiciones: `kafka`, `kafka-ui` y `orders-api` en ejecución (`http://localhost:8081` para Kafka UI).
- Datos de entrada:

```json
{
  "customerName": "Pedro",
  "product": "Libro de arquitectura",
  "quantity": 1,
  "unitPrice": 85000
}
```

- Pasos ejecutados:
  1. Enviar pedido.
  2. Revisar logs de `orders-api`.
  3. Revisar el topic en Kafka UI.
- Resultado esperado: Se observa un evento `OrderCreated` con `eventId`, `orderId` y `correlationId`.
- Resultado obtenido:
- Evidencia:
- Estado: Cumple / No cumple
- Análisis técnico:

### CP-05 Consumo de evento (Kafka)

- Arquitectura: Kafka
- Objetivo: Validar que los consumidores reciben el evento publicado.
- Precondiciones: Consumidores en ejecución.
- Datos de entrada: Pedido enviado a `POST /orders`.
- Pasos ejecutados:
  1. Enviar pedido.
  2. Revisar logs de `inventory-consumer`.
  3. Revisar logs de `billing-consumer`.
  4. Revisar logs de `notification-consumer`.
- Resultado esperado: Los tres consumidores registran el mismo `orderId`.
- Resultado obtenido:
- Evidencia:
- Estado: Cumple / No cumple
- Análisis técnico:

### CP-06 Trazabilidad del flujo

- Arquitectura: RabbitMQ / Kafka
- Objetivo: Confirmar el seguimiento completo desde la API hasta el consumidor.
- Precondiciones: Stack levantado.
- Datos de entrada: Pedido de prueba.
- Pasos ejecutados:
  1. Enviar pedido.
  2. Registrar `orderId`.
  3. Buscar ese `orderId` en logs y consola del broker.
- Resultado esperado: El flujo completo es rastreable mediante `orderId` y `correlationId`.
- Resultado obtenido:
- Evidencia:
- Estado: Cumple / No cumple
- Análisis técnico:

### CP-07 Responsive

- Arquitectura: RabbitMQ / Kafka
- Objetivo: Validar que la API responde sin esperar el procesamiento del consumidor.
- Precondiciones: Stack levantado.
- Datos de entrada: Pedido de prueba.
- Pasos ejecutados:
  1. Enviar pedido.
  2. Observar tiempo de respuesta HTTP.
  3. Comparar con logs de procesamiento posterior.
- Resultado esperado: La respuesta llega antes de que el consumidor termine su trabajo.
- Resultado obtenido:
- Evidencia:
- Estado: Cumple / No cumple
- Análisis técnico:

### CP-08 Message-driven

- Arquitectura: RabbitMQ / Kafka
- Objetivo: Verificar que no existe comunicación directa entre API y consumidores.
- Precondiciones: Revisar contenedores y código.
- Datos de entrada: N/A
- Pasos ejecutados:
  1. Revisar endpoints expuestos por los consumidores.
  2. Revisar flujo de comunicación.
- Resultado esperado: Toda interacción ocurre a través del broker.
- Resultado obtenido:
- Evidencia:
- Estado: Cumple / No cumple
- Análisis técnico:

### CP-09 Resilient

- Arquitectura: RabbitMQ / Kafka
- Objetivo: Validar el comportamiento cuando un consumidor está apagado.
- Precondiciones: Stack levantado.
- Datos de entrada: Pedido de prueba.
- Pasos ejecutados:
  1. Apagar un consumidor.
  2. Enviar pedido.
  3. Observar comportamiento del broker.
  4. Levantar nuevamente el consumidor.
- Resultado esperado: El productor sigue funcionando y el procesamiento puede ocurrir después.
- Resultado obtenido:
- Evidencia:
- Estado: Cumple / No cumple
- Análisis técnico:

### CP-10 Elastic

- Arquitectura: RabbitMQ / Kafka
- Objetivo: Verificar distribución de carga con múltiples consumidores.
- Precondiciones: Stack levantado.
- Datos de entrada: Varios pedidos consecutivos.
- Pasos ejecutados:
  1. Escalar consumidores.
  2. Enviar múltiples pedidos.
  3. Revisar logs por instancia.
- Resultado esperado: El trabajo se distribuye entre instancias consumidoras.
- Resultado obtenido:
- Evidencia:
- Estado: Cumple / No cumple
- Análisis técnico:

### CP-11 Desacoplamiento temporal

- Arquitectura: RabbitMQ / Kafka
- Objetivo: Validar que el productor puede publicar aunque el consumidor procese después.
- Precondiciones: Consumidor detenido temporalmente.
- Datos de entrada: Pedido de prueba.
- Pasos ejecutados:
  1. Detener consumidor.
  2. Enviar pedido.
  3. Levantar consumidor.
  4. Revisar procesamiento posterior.
- Resultado esperado: La publicación no depende del tiempo de disponibilidad del consumidor.
- Resultado obtenido:
- Evidencia:
- Estado: Cumple / No cumple
- Análisis técnico:

### CP-12 Recuperación

- Arquitectura: RabbitMQ / Kafka
- Objetivo: Verificar reprocesamiento o reanudación tras reiniciar consumidores.
- Precondiciones: Stack levantado.
- Datos de entrada: Pedido de prueba.
- Pasos ejecutados:
  1. Reiniciar consumidor.
  2. Revisar reconexión.
  3. Enviar nuevo pedido o validar evento pendiente.
- Resultado esperado: El consumidor vuelve a operar y retoma el flujo.
- Resultado obtenido:
- Evidencia:
- Estado: Cumple / No cumple
- Análisis técnico:

## 5. Conclusiones

### 5.1 ¿Qué arquitectura fue más robusta desde QA?

### 5.2 ¿Qué dificultades se presentaron durante las pruebas?

### 5.3 ¿Qué diferencias reales se evidenciaron entre RabbitMQ y Kafka?

## 6. Anexos

- Capturas
- Logs
- Comandos ejecutados
- Observaciones finales
