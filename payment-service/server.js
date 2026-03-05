const { connectWithRetry, getRetryCount } = require("/app/shared/rabbit");

const QUEUE = "payments.queue";
const RESULTS_EXCHANGE = "results.payment";
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES) || 3;
const FAIL_RATE = parseInt(process.env.PAYMENT_FAIL_RATE) || 20;

async function main() {
  const { connection, channel } = await connectWithRetry(
    process.env.RABBITMQ_URL,
  );
  await channel.prefetch(1);

  console.log(`[Payment] Consuming from ${QUEUE}, fail rate: ${FAIL_RATE}%`);

  channel.consume(QUEUE, async (msg) => {
    if (!msg) return;

    const order = JSON.parse(msg.content.toString());
    const correlationId = msg.properties.headers?.correlationId;
    const retryCount = getRetryCount(msg);

    console.log(
      `[Payment] Processing ${correlationId} (attempt ${retryCount + 1})`,
    );

    try {
      // Simulate success/failure based on FAIL_RATE
      const roll = Math.floor(Math.random() * 100) + 1; // 1..100
      const ok = roll > FAIL_RATE;

      if (!ok) {
        throw new Error(
          `Payment failed (roll=${roll}, failRate=${FAIL_RATE}%)`,
        );
      }

      // On success: publish result event and ack
      const resultEvent = {
        correlationId,
        orderId: order.orderId,
        status: "PAID",
        processedAt: new Date().toISOString(),
      };

      channel.publish(
        RESULTS_EXCHANGE,
        "",
        Buffer.from(JSON.stringify(resultEvent)),
        {
          persistent: true,
          headers: {
            correlationId,
          },
        },
      );

      channel.ack(msg);
      console.log(
        `[Payment] ✅ Paid ${correlationId} → published to ${RESULTS_EXCHANGE}`,
      );
    } catch (err) {
      // Retry / DLQ logic (provided as reference — study this for other services)
      if (retryCount >= MAX_RETRIES - 1) {
        channel.publish(process.env.DLQ_EXCHANGE, "", msg.content, {
          headers: msg.properties.headers,
        });
        channel.ack(msg);
        console.log(
          `[Payment] → DLQ after ${retryCount + 1} attempts: ${err.message}`,
        );
      } else {
        channel.nack(msg, false, false);
        console.log(
          `[Payment] → Retry (attempt ${retryCount + 1}): ${err.message}`,
        );
      }
    }
  });
}

main().catch(console.error);
