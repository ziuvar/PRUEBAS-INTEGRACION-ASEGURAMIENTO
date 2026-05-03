const express = require('express');
const { Kafka } = require('kafkajs');
const { randomUUID } = require('crypto');

const app = express();

const port = Number(process.env.PORT || 3000);
const kafkaBroker = process.env.KAFKA_BROKER || 'kafka:9092';
const kafkaTopic = process.env.KAFKA_TOPIC || 'orders.events';
const kafkaPartitions = Number(process.env.KAFKA_PARTITIONS || 2);

const kafka = new Kafka({
  clientId: 'orders-api',
  brokers: [kafkaBroker]
});

const producer = kafka.producer();

let brokerReady = false;

app.use(express.json());

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function validateOrder(body) {
  if (!body || typeof body !== 'object') {
    return 'El cuerpo debe ser un JSON valido.';
  }

  if (!body.customerName || typeof body.customerName !== 'string') {
    return 'customerName es obligatorio.';
  }

  if (!body.product || typeof body.product !== 'string') {
    return 'product es obligatorio.';
  }

  if (!Number.isInteger(body.quantity) || body.quantity <= 0) {
    return 'quantity debe ser un entero mayor que cero.';
  }

  if (typeof body.unitPrice !== 'number' || body.unitPrice <= 0) {
    return 'unitPrice debe ser un numero mayor que cero.';
  }

  return null;
}

async function ensureTopic() {
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({
    waitForLeaders: true,
    topics: [
      {
        topic: kafkaTopic,
        numPartitions: kafkaPartitions,
        replicationFactor: 1
      }
    ]
  });
  await admin.disconnect();
}

async function connectKafka() {
  while (!brokerReady) {
    try {
      console.log(`[orders-api][kafka] Intentando conexion a ${kafkaBroker}`);
      await ensureTopic();
      await producer.connect();
      brokerReady = true;
      console.log(
        `[orders-api][kafka] Conexion lista y topic "${kafkaTopic}" preparado con ${kafkaPartitions} particiones.`
      );
    } catch (error) {
      brokerReady = false;
      console.error(`[orders-api][kafka] Kafka no disponible: ${error.message}`);
      await sleep(3000);
    }
  }
}

app.get('/health', (_req, res) => {
  res.json({
    service: 'orders-api',
    architecture: 'kafka',
    brokerReady,
    kafkaTopic,
    kafkaPartitions
  });
});

app.post('/orders', async (req, res) => {
  const validationError = validateOrder(req.body);
  if (validationError) {
    return res.status(400).json({ status: 'error', message: validationError });
  }

  if (!brokerReady) {
    return res.status(503).json({
      status: 'error',
      message: 'Kafka aun no esta disponible.'
    });
  }

  const orderId = `ORD-${Date.now()}`;
  const correlationId = randomUUID();
  const total = req.body.quantity * req.body.unitPrice;
  const event = {
    eventId: randomUUID(),
    eventType: 'OrderCreated',
    eventVersion: '1.0',
    occurredAt: new Date().toISOString(),
    source: 'orders-api',
    correlationId,
    data: {
      orderId,
      customerName: req.body.customerName,
      product: req.body.product,
      quantity: req.body.quantity,
      unitPrice: req.body.unitPrice,
      total
    }
  };

  await producer.send({
    topic: kafkaTopic,
    messages: [
      {
        key: orderId,
        value: JSON.stringify(event)
      }
    ]
  });

  console.log(
    `[orders-api][kafka] Evento OrderCreated publicado orderId=${orderId} correlationId=${correlationId}.`
  );

  return res.status(202).json({
    status: 'accepted',
    message: 'Pedido recibido y evento OrderCreated publicado en Kafka',
    orderId
  });
});

app.listen(port, () => {
  console.log(`[orders-api][kafka] API escuchando en puerto ${port}`);
});

connectKafka().catch((error) => {
  console.error('[orders-api][kafka] Error fatal:', error.message);
  process.exit(1);
});

async function shutdown() {
  console.log('[orders-api][kafka] Cerrando servicio...');
  try {
    await producer.disconnect();
  } finally {
    process.exit(0);
  }
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

