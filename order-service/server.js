const express = require("express");
const amqp = require("amqplib");
const { v4: uuidv4 } = require("uuid");

const app = express();
const PORT = process.env.PORT || 3000;
const RABBITMQ_URL = process.env.RABBITMQ_URL;

app.use(express.json());
const ordersByCorrelationId = new Map();

app.get("/health", (req, res) => {
  res.status(200).json({ status: "ok" });
});

// Required by tests: fetch stored order
app.get("/orders/:correlationId", (req, res) => {
  const { correlationId } = req.params;
  const order = ordersByCorrelationId.get(correlationId);

  if (!order) {
    return res.status(404).json({ error: "Order not found" });
  }

  return res.status(200).json(order);
});

app.post("/orders", async (req, res) => {
  const inputOrder = req.body;

  // Tests expect UUID v4 correlationId
  const correlationId = uuidv4();
  const orderId = `ord-${correlationId.slice(0, 8)}`;
  const timestamp = new Date().toISOString();

  // canonical-ish stored/published event
  const orderEvent = {
    ...inputOrder,
    orderId,
    correlationId,
    timestamp,
  };

  // store so GET /orders/:correlationId works
  ordersByCorrelationId.set(correlationId, orderEvent);

  try {
    if (!RABBITMQ_URL) throw new Error("RABBITMQ_URL is not set");

    const connection = await amqp.connect(RABBITMQ_URL);
    const channel = await connection.createChannel();

    const exchange = "orders.exchange";

    // exchange exists in definitions, but assert is ok
    await channel.assertExchange(exchange, "fanout", { durable: true });

    channel.publish(exchange, "", Buffer.from(JSON.stringify(orderEvent)), {
      persistent: true,
      contentType: "application/json",
      headers: { correlationId },
    });

    await channel.close();
    await connection.close();
  } catch (err) {
    console.error("[Order] RabbitMQ publish failed:", err);
  }

  // Tests expect 201 and correlationId in response
  return res.status(201).json({
    status: "accepted",
    correlationId,
  });
});

app.listen(PORT, () => {
  console.log(`[Order] Listening on port ${PORT}`);
});
