const amqp = require('amqplib');

const rabbitUrl = process.env.RABBITMQ_URL || 'amqp://admin:admin@rabbitmq:5672';
const queueName = process.env.RABBITMQ_QUEUE || 'pedidos';
const prefetch = Number(process.env.PREFETCH || 1);
const processingDelayMs = Number(process.env.PROCESSING_DELAY_MS || 2000);
const workerName = process.env.WORKER_NAME || process.env.HOSTNAME || 'notification-worker';

let connection = null;
let channel = null;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function startWorker() {
  while (!channel) {
    try {
      console.log(`[${workerName}][rabbitmq] Intentando conexion a ${rabbitUrl}`);
      connection = await amqp.connect(rabbitUrl);
      channel = await connection.createChannel();
      await channel.assertQueue(queueName, { durable: true });
      await channel.prefetch(prefetch);

      console.log(
        `[${workerName}][rabbitmq] Escuchando cola "${queueName}" con prefetch=${prefetch}.`
      );

      connection.on('close', () => {
        console.error(`[${workerName}][rabbitmq] Conexion cerrada. Reintentando...`);
        channel = null;
        setTimeout(() => {
          startWorker().catch((error) => {
            console.error(`[${workerName}][rabbitmq] Error al reiniciar: ${error.message}`);
          });
        }, 3000);
      });

      connection.on('error', (error) => {
        console.error(`[${workerName}][rabbitmq] Error de conexion: ${error.message}`);
      });

      await channel.consume(queueName, async (msg) => {
        if (!msg) {
          return;
        }

        try {
          const payload = JSON.parse(msg.content.toString());
          console.log(
            `[${workerName}][rabbitmq] Mensaje recibido orderId=${payload.orderId} correlationId=${payload.correlationId}`
          );
          console.log(`[${workerName}][rabbitmq] Simulando envio de notificacion...`);

          await sleep(processingDelayMs);

          console.log(
            `[${workerName}][rabbitmq] Pedido ${payload.orderId} procesado y confirmado con ack manual.`
          );
          channel.ack(msg);
        } catch (error) {
          console.error(`[${workerName}][rabbitmq] Error procesando mensaje: ${error.message}`);
          channel.nack(msg, false, false);
        }
      });
    } catch (error) {
      channel = null;
      console.error(`[${workerName}][rabbitmq] RabbitMQ no disponible: ${error.message}`);
      await sleep(3000);
    }
  }
}

startWorker().catch((error) => {
  console.error(`[${workerName}][rabbitmq] Error fatal: ${error.message}`);
  process.exit(1);
});

async function shutdown() {
  console.log(`[${workerName}][rabbitmq] Cerrando worker...`);
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

