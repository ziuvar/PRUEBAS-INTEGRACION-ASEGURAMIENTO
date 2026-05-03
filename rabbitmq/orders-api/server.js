const express = require('express');
const amqp = require('amqplib');
const { randomUUID } = require('crypto');

const app = express();

const port = Number(process.env.PORT || 3000);
const rabbitUrl = process.env.RABBITMQ_URL || 'amqp://admin:admin@rabbitmq:5672';
const queueName = process.env.RABBITMQ_QUEUE || 'pedidos';

let connection = null;
let channel = null;
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

  return null;
}

async function connectToRabbit() {
  while (!brokerReady) {
    try {
      console.log(`[orders-api][rabbitmq] Intentando conexion a ${rabbitUrl}`);
      connection = await amqp.connect(rabbitUrl);
      channel = await connection.createChannel();
      await channel.assertQueue(queueName, { durable: true });
      brokerReady = true;
      console.log(`[orders-api][rabbitmq] Conexion lista y cola "${queueName}" disponible.`);

      connection.on('close', () => {
        brokerReady = false;
        console.error('[orders-api][rabbitmq] Conexion cerrada. Reintentando...');
        setTimeout(() => {
          connectToRabbit().catch((error) => {
            console.error('[orders-api][rabbitmq] Error al reconectar:', error.message);
          });
        }, 3000);
      });

      connection.on('error', (error) => {
        brokerReady = false;
        console.error('[orders-api][rabbitmq] Error de conexion:', error.message);
      });
    } catch (error) {
      brokerReady = false;
      console.error(`[orders-api][rabbitmq] RabbitMQ no disponible: ${error.message}`);
      await sleep(3000);
    }
  }
}

app.get('/health', (_req, res) => {
  res.json({
    service: 'orders-api',
    architecture: 'rabbitmq',
    brokerReady,
    queueName
  });
});

app.post('/orders', async (req, res) => {
  const validationError = validateOrder(req.body);
  if (validationError) {
    return res.status(400).json({ status: 'error', message: validationError });
  }

  if (!brokerReady || !channel) {
    return res.status(503).json({
      status: 'error',
      message: 'RabbitMQ aun no esta disponible.'
    });
  }

  const orderId = `ORD-${Date.now()}`;
  const correlationId = randomUUID();
  const message = {
    type: 'OrderCreatedMessage',
    orderId,
    customerName: req.body.customerName,
    product: req.body.product,
    quantity: req.body.quantity,
    createdAt: new Date().toISOString(),
    correlationId
  };

  channel.sendToQueue(queueName, Buffer.from(JSON.stringify(message)), {
    persistent: true,
    contentType: 'application/json',
    messageId: orderId,
    correlationId
  });

  console.log(
    `[orders-api][rabbitmq] Pedido ${orderId} publicado en cola "${queueName}" con correlationId=${correlationId}.`
  );

  return res.status(202).json({
    status: 'accepted',
    message: 'Pedido recibido y enviado a RabbitMQ',
    orderId
  });
});

app.listen(port, () => {
  console.log(`[orders-api][rabbitmq] API escuchando en puerto ${port}`);
});

connectToRabbit().catch((error) => {
  console.error('[orders-api][rabbitmq] Error fatal de conexion:', error.message);
  process.exit(1);
});

async function shutdown() {
  console.log('[orders-api][rabbitmq] Cerrando servicio...');
  try {
    if (channel) {
      await channel.close();
    }
    if (connection) {
      await connection.close();
    }
  } finally {
    process.exit(0);
  }
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

