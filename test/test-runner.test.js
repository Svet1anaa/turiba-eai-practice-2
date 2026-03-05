/* eslint-disable no-console */
const fs = require('fs');
const path = require('path');
const { exec } = require('child_process');
const axios = require('axios');
const amqp = require('amqplib');
const {
  connectToRabbit,
  consumeFromQueue,
  getQueueInfo,
  getExchangeInfo,
  purgeQueue
} = require('./helpers/rabbit-helper');

const ORDER_SERVICE_URL = process.env.ORDER_SERVICE_URL || 'http://localhost:3000';
const RABBIT_MGMT_URL = process.env.RABBITMQ_MGMT_URL || 'http://localhost:15672/api/overview';

const COMPOSE_FILE = path.resolve(__dirname, '..', 'docker-compose.yml');
const ROOT_DIR = path.resolve(__dirname, '..');
const NOTIFICATION_LOG_FILE = path.resolve(ROOT_DIR, 'notification-service', 'data', 'notification.log');
const PROCESSED_IDS_FILE = path.resolve(ROOT_DIR, 'notification-service', 'data', 'processed-ids.json');

const resultQueues = ['payment.results', 'inventory.results', 'notification.results'];
const allQueues = [
  'payments.queue',
  'inventory.queue',
  'notifications.queue',
  'payments.retry.queue',
  'inventory.retry.queue',
  'notifications.retry.queue',
  'orders.dlq',
  ...resultQueues
];

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function runCommand(command, envOverrides = {}) {
  return new Promise((resolve, reject) => {
    exec(command, {
      cwd: ROOT_DIR,
      env: { ...process.env, ...envOverrides }
    }, (error, stdout, stderr) => {
      if (error) {
        reject(new Error(`${command}\n${stderr || stdout || error.message}`));
        return;
      }
      resolve({ stdout, stderr });
    });
  });
}

async function restartService(serviceName, envOverrides = {}) {
  await runCommand(`docker-compose -f "${COMPOSE_FILE}" stop ${serviceName}`);

  const cmd = `docker-compose -f "${COMPOSE_FILE}" up -d ${serviceName}`;
  await runCommand(cmd, envOverrides);
  await sleep(4000);
}

async function postOrder() {
  const payload = {
    customerId: 'cust-123',
    items: [
      { productId: 'prod-001', quantity: 2, unitPrice: 29.99 },
      { productId: 'prod-002', quantity: 1, unitPrice: 149.99 }
    ],
    totalAmount: 209.97,
    orderType: 'standard'
  };

  const response = await axios.post(`${ORDER_SERVICE_URL}/orders`, payload, {
    validateStatus: () => true,
    timeout: 10000
  });

  return { response, payload };
}

function isUuidV4(value) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

async function purgeKnownQueues() {
  for (const queueName of allQueues) {
    try {
      await purgeQueue(queueName);
    } catch (err) {
      console.warn(`[Test] Failed to purge ${queueName}: ${err.message}`);
    }
  }
}

describe('Practice 2 Event-Driven Messaging', () => {
  jest.setTimeout(120000);

  afterAll(async () => {
    // Best effort: restore defaults expected by other tests.
    try {
      await restartService('payment-service', { PAYMENT_FAIL_RATE: '20' });
    } catch (err) {
      console.warn(`[Test] Payment service reset warning: ${err.message}`);
    }

    try {
      await restartService('inventory-service', { INVENTORY_FAIL_RATE: '10' });
    } catch (err) {
      console.warn(`[Test] Inventory service reset warning: ${err.message}`);
    }

    try {
      await restartService('notification-service');
    } catch (err) {
      console.warn(`[Test] Notification service reset warning: ${err.message}`);
    }
  });

  test('1) services start without errors', async () => {
    const health = await axios.get(`${ORDER_SERVICE_URL}/health`, {
      validateStatus: () => true,
      timeout: 10000
    });
    expect(health.status).toBe(200);

    const mgmt = await axios.get(RABBIT_MGMT_URL, {
      auth: { username: 'guest', password: 'guest' },
      validateStatus: () => true,
      timeout: 10000
    });
    expect(mgmt.status).toBe(200);

    for (const queueName of ['payments.queue', 'inventory.queue', 'notifications.queue']) {
      const info = await getQueueInfo(queueName);
      expect(info.consumers).toBeGreaterThan(0);
    }
  });

  test('2) POST /orders returns valid correlation ID and order can be fetched', async () => {
    const { response } = await postOrder();
    expect(response.status).toBe(201);
    expect(response.data).toBeDefined();
    expect(response.data.status).toBe('accepted');
    expect(typeof response.data.correlationId).toBe('string');
    expect(isUuidV4(response.data.correlationId)).toBe(true);

    const getResp = await axios.get(`${ORDER_SERVICE_URL}/orders/${response.data.correlationId}`, {
      validateStatus: () => true,
      timeout: 10000
    });
    expect(getResp.status).toBe(200);
    expect(getResp.data.correlationId).toBe(response.data.correlationId);
  });

  test('3) exchange and queue topology is correct', async () => {
    const exchange = await getExchangeInfo('orders.exchange');
    expect(exchange).toBeDefined();
    expect(exchange.type).toBe('fanout');

    for (const queueName of ['payments.queue', 'inventory.queue', 'notifications.queue']) {
      const info = await getQueueInfo(queueName);
      expect(info).toBeDefined();
      expect(info.consumers).toBeGreaterThanOrEqual(1);
    }
  });

  test('4) DLQ receives failed messages after retries', async () => {
    await purgeKnownQueues();
    await restartService('payment-service', { PAYMENT_FAIL_RATE: '100' });

    const { response } = await postOrder();
    expect(response.status).toBe(201);
    const expectedCorrelationId = response.data.correlationId;

    await sleep(6000);

    const { connection, channel } = await connectToRabbit();
    const dlqMessages = await consumeFromQueue(channel, 'orders.dlq', 10000, 3);
    await channel.close();
    await connection.close();

    expect(dlqMessages.length).toBeGreaterThan(0);

    const matching = dlqMessages.find((m) => m.properties?.headers?.correlationId === expectedCorrelationId);
    expect(matching).toBeDefined();
  });

  test('5) correlation ID is propagated through all consumers', async () => {
    await purgeKnownQueues();
    await restartService('payment-service', { PAYMENT_FAIL_RATE: '0' });
    await restartService('inventory-service', { INVENTORY_FAIL_RATE: '0' });

    const { response } = await postOrder();
    expect(response.status).toBe(201);

    const expectedCorrelationId = response.data.correlationId;

    const { connection, channel } = await connectToRabbit();

    const paymentMsgs = await consumeFromQueue(channel, 'payment.results', 10000, 5);
    const inventoryMsgs = await consumeFromQueue(channel, 'inventory.results', 10000, 5);
    const notificationMsgs = await consumeFromQueue(channel, 'notification.results', 10000, 5);

    await channel.close();
    await connection.close();

    const payment = paymentMsgs.find((m) => m.properties?.headers?.correlationId === expectedCorrelationId);
    const inventory = inventoryMsgs.find((m) => m.properties?.headers?.correlationId === expectedCorrelationId);
    const notification = notificationMsgs.find((m) => m.properties?.headers?.correlationId === expectedCorrelationId);

    expect(payment).toBeDefined();
    expect(inventory).toBeDefined();
    expect(notification).toBeDefined();
  });

  test('6) notification service is idempotent', async () => {
    await purgeKnownQueues();

    try {
      fs.unlinkSync(NOTIFICATION_LOG_FILE);
    } catch {}
    try {
      fs.unlinkSync(PROCESSED_IDS_FILE);
    } catch {}

    await restartService('notification-service');

    const { response } = await postOrder();
    expect(response.status).toBe(201);
    const correlationId = response.data.correlationId;

    await sleep(3000);

    const logContent = fs.readFileSync(NOTIFICATION_LOG_FILE, 'utf8').trim();
    const lines = logContent ? logContent.split('\n') : [];
    expect(lines.length).toBe(1);

    const firstEntry = JSON.parse(lines[0]);
    expect(firstEntry.correlationId).toBe(correlationId);

    const processedRaw = fs.readFileSync(PROCESSED_IDS_FILE, 'utf8');
    const processedIds = JSON.parse(processedRaw);
    expect(Array.isArray(processedIds)).toBe(true);
    expect(processedIds).toContain(correlationId);

    const { connection, channel } = await connectToRabbit();

    const duplicateOrder = {
      orderId: firstEntry.orderId,
      correlationId,
      customerId: firstEntry.customerId,
      items: [],
      totalAmount: 0,
      orderType: 'standard',
      timestamp: new Date().toISOString()
    };

    channel.sendToQueue('notifications.queue', Buffer.from(JSON.stringify(duplicateOrder)), {
      headers: {
        correlationId
      },
      contentType: 'application/json'
    });

    await channel.close();
    await connection.close();

    await sleep(3000);

    const secondLogContent = fs.readFileSync(NOTIFICATION_LOG_FILE, 'utf8').trim();
    const secondLines = secondLogContent ? secondLogContent.split('\n') : [];
    expect(secondLines.length).toBe(1);
  });

  test('7) retry count indicates message retried before DLQ', async () => {
    await purgeKnownQueues();
    await restartService('payment-service', { PAYMENT_FAIL_RATE: '100' });

    const { response } = await postOrder();
    expect(response.status).toBe(201);
    const correlationId = response.data.correlationId;

    await sleep(6000);

    const conn = await amqp.connect(process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672');
    const channel = await conn.createChannel();

    const deadline = Date.now() + 10000;
    let found = null;
    while (Date.now() < deadline && !found) {
      const msg = await channel.get('orders.dlq', { noAck: false });
      if (msg) {
        const headerCorrelationId = msg.properties?.headers?.correlationId;
        if (headerCorrelationId === correlationId) {
          found = msg;
        }
        channel.ack(msg);
      } else {
        await sleep(200);
      }
    }

    await channel.close();
    await conn.close();

    expect(found).toBeDefined();
    const xDeath = found.properties?.headers?.['x-death'];
    expect(Array.isArray(xDeath)).toBe(true);

    const totalCount = xDeath.reduce((sum, death) => sum + (Number(death.count) || 0), 0);
    expect(totalCount).toBeGreaterThanOrEqual(2);
  });
});

