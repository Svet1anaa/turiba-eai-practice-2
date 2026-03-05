const { connectWithRetry, getRetryCount } = require("/app/shared/rabbit");

const QUEUE = "inventory.queue";
const RESULTS_EXCHANGE = "results.inventory";
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES) || 3;
const FAIL_RATE = parseInt(process.env.INVENTORY_FAIL_RATE) || 20;

async function main() {
  const { connection, channel } = await connectWithRetry(
    process.env.RABBITMQ_URL,
  );
  await channel.prefetch(1);

  console.log(`[Inventory] Consuming from ${QUEUE}, fail rate: ${FAIL_RATE}%`);

  channel.consume(QUEUE, async (msg) => {
    if (!msg) return;

    const orderEvent = JSON.parse(msg.content.toString());
    const correlationId = msg.properties.headers?.correlationId;
    const retryCount = getRetryCount(msg);

    console.log(
      `[Inventory] Processing ${correlationId} (attempt ${retryCount + 1})`,
    );

    try {
      // Simulate reserve success/failure
      const roll = Math.floor(Math.random() * 100) + 1; // 1..100
      const ok = roll > FAIL_RATE;

      if (!ok) {
        throw new Error(
          `Inventory reservation failed (roll=${roll}, failRate=${FAIL_RATE}%)`,
        );
      }

      const resultEvent = {
        correlationId,
        orderId: orderEvent.orderId,
        status: "RESERVED",
        processedAt: new Date().toISOString(),
      };

      channel.publish(
        RESULTS_EXCHANGE,
        "",
        Buffer.from(JSON.stringify(resultEvent)),
        { persistent: true, headers: { correlationId } },
      );

      channel.ack(msg);
      console.log(
        `[Inventory] ✅ Reserved ${correlationId} → published to ${RESULTS_EXCHANGE}`,
      );
    } catch (err) {
      if (retryCount >= MAX_RETRIES - 1) {
        channel.publish(process.env.DLQ_EXCHANGE, "", msg.content, {
          headers: msg.properties.headers,
        });
        channel.ack(msg);
        console.log(
          `[Inventory] → DLQ after ${retryCount + 1} attempts: ${err.message}`,
        );
      } else {
        channel.nack(msg, false, false);
        console.log(
          `[Inventory] → Retry (attempt ${retryCount + 1}): ${err.message}`,
        );
      }
    }
  });
}

main().catch(console.error);
