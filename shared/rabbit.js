const amqp = require('amqplib');

const DEFAULT_RETRIES = 10;
const DEFAULT_DELAY = 3000;
const MAX_DELAY = 30000;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function connectWithRetry(url, retries = DEFAULT_RETRIES, delay = DEFAULT_DELAY) {
  let attempt = 0;
  let lastError;

  while (attempt < retries) {
    try {
      const connection = await amqp.connect(url);
      const channel = await connection.createChannel();

      connection.on('error', (err) => {
        console.error(`[RabbitMQ] Connection error: ${err.message}`);
      });

      connection.on('close', () => {
        console.warn('[RabbitMQ] Connection closed');
      });

      return { connection, channel };
    } catch (err) {
      lastError = err;
      attempt += 1;

      const waitMs = Math.min(delay * Math.pow(2, attempt - 1), MAX_DELAY);
      console.warn(
        `[RabbitMQ] Connection attempt ${attempt}/${retries} failed: ${err.message}. Retrying in ${waitMs}ms`
      );

      if (attempt >= retries) {
        break;
      }

      await sleep(waitMs);
    }
  }

  throw new Error(
    `[RabbitMQ] Unable to connect after ${retries} attempts: ${lastError ? lastError.message : 'Unknown error'}`
  );
}

function getRetryCount(msg) {
  const xDeath = msg?.properties?.headers?.['x-death'];

  if (!Array.isArray(xDeath)) {
    return 0;
  }

  return xDeath.reduce((sum, entry) => sum + (Number(entry?.count) || 0), 0);
}

module.exports = {
  connectWithRetry,
  getRetryCount
};

