const { Kafka } = require('kafkajs');

const serviceName = process.env.SERVICE_NAME || 'notification-consumer';
const actionLabel = process.env.ACTION_LABEL || 'Procesamiento de evento';
const kafkaBroker = process.env.KAFKA_BROKER || 'kafka:9092';
const kafkaTopic = process.env.KAFKA_TOPIC || 'orders.events';
const groupId = process.env.KAFKA_GROUP_ID || 'notification-service-group';
const kafkaPartitions = Number(process.env.KAFKA_PARTITIONS || 2);
const fromBeginning = String(process.env.FROM_BEGINNING || 'true') === 'true';
const processingDelayMs = Number(process.env.PROCESSING_DELAY_MS || 1000);
const instanceId = process.env.HOSTNAME || 'local-instance';

const kafka = new Kafka({
  clientId: serviceName,
  brokers: [kafkaBroker]
});

const consumer = kafka.consumer({ groupId });

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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

async function startConsumer() {
  while (true) {
    try {
      console.log(`[${serviceName}][kafka] Intentando conexion a ${kafkaBroker}`);
      await ensureTopic();
      await consumer.connect();
      await consumer.subscribe({ topic: kafkaTopic, fromBeginning });

      console.log(
        `[${serviceName}][kafka] Escuchando topic "${kafkaTopic}" en groupId="${groupId}" desde instanceId="${instanceId}".`
      );

      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          const payload = JSON.parse(message.value.toString());
          const orderId = payload?.data?.orderId || 'sin-order-id';

          console.log(
            `[${serviceName}][kafka] Evento recibido eventType=${payload.eventType} orderId=${orderId}`
          );
          console.log(
            `[${serviceName}][kafka] ${actionLabel} topic=${topic} partition=${partition} offset=${message.offset} key=${message.key?.toString()}`
          );

          await sleep(processingDelayMs);

          console.log(
            `[${serviceName}][kafka] Procesamiento completado para orderId=${orderId} correlationId=${payload.correlationId}`
          );
        }
      });

      break;
    } catch (error) {
      console.error(`[${serviceName}][kafka] Kafka no disponible: ${error.message}`);
      await sleep(3000);
    }
  }
}

startConsumer().catch((error) => {
  console.error(`[${serviceName}][kafka] Error fatal: ${error.message}`);
  process.exit(1);
});

async function shutdown() {
  console.log(`[${serviceName}][kafka] Cerrando consumidor...`);
  try {
    await consumer.disconnect();
  } finally {
    process.exit(0);
  }
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
