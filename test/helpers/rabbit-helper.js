const amqp = require('amqplib');
const axios = require('axios');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const MGMT_BASE_URL = process.env.RABBITMQ_MGMT_URL || 'http://localhost:15672/api';
const MGMT_USER = process.env.RABBITMQ_USER || 'guest';
const MGMT_PASS = process.env.RABBITMQ_PASS || 'guest';

const mgmt = axios.create({
  baseURL: MGMT_BASE_URL,
  auth: {
    username: MGMT_USER,
    password: MGMT_PASS
  },
  timeout: 15000
});

async function connectToRabbit() {
  const connection = await amqp.connect(RABBITMQ_URL);
  const channel = await connection.createChannel();
  return { connection, channel };
}

async function consumeFromQueue(channel, queueName, timeout = 10000, maxMessages = 1) {
  const messages = [];
  const deadline = Date.now() + timeout;

  while (Date.now() < deadline && messages.length < maxMessages) {
    const msg = await channel.get(queueName, { noAck: false });

    if (msg) {
      const payloadText = msg.content.toString();
      let payload;

      try {
        payload = JSON.parse(payloadText);
      } catch {
        payload = payloadText;
      }

      messages.push({
        payload,
        properties: msg.properties,
        fields: msg.fields
      });

      channel.ack(msg);
      continue;
    }

    await new Promise((resolve) => setTimeout(resolve, 200));
  }

  return messages;
}

function encodeName(name) {
  return encodeURIComponent(name);
}

async function getQueueInfo(queueName) {
  const { data } = await mgmt.get(`/queues/%2F/${encodeName(queueName)}`);
  return data;
}

async function getExchangeInfo(exchangeName) {
  const { data } = await mgmt.get(`/exchanges/%2F/${encodeName(exchangeName)}`);
  return data;
}

async function getBindings(exchangeName) {
  const { data } = await mgmt.get(`/exchanges/%2F/${encodeName(exchangeName)}/bindings/source`);
  return data;
}

async function purgeQueue(queueName) {
  await mgmt.delete(`/queues/%2F/${encodeName(queueName)}/contents`);
}

module.exports = {
  connectToRabbit,
  consumeFromQueue,
  getQueueInfo,
  getExchangeInfo,
  getBindings,
  purgeQueue
};

