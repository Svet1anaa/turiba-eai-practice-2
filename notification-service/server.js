const fs = require("fs");
const { connectWithRetry, getRetryCount } = require("/app/shared/rabbit");

const QUEUE = "notifications.queue";
const RESULTS_EXCHANGE = "results.notification";
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES) || 3;
const FAIL_RATE = parseInt(process.env.NOTIFICATION_FAIL_RATE) || 10;

// These paths MUST match the mounted host folder (notification-service/data)
const DATA_DIR = "/data";
const PROCESSED_IDS_FILE = `${DATA_DIR}/processed-ids.json`;
const NOTIFICATION_LOG_FILE = `${DATA_DIR}/notification.log`;

function loadProcessedIds() {
  try {
    const raw = fs.readFileSync(PROCESSED_IDS_FILE, "utf8");
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr : [];
  } catch {
    return [];
  }
}

function saveProcessedIds(arr) {
  fs.writeFileSync(PROCESSED_IDS_FILE, JSON.stringify(arr, null, 2), "utf8");
}

function appendNotificationLog(entryObj) {
  // JSON Lines format: one JSON per line
  fs.appendFileSync(
    NOTIFICATION_LOG_FILE,
    JSON.stringify(entryObj) + "\n",
    "utf8",
  );
}

async function main() {
  const { channel } = await connectWithRetry(process.env.RABBITMQ_URL);
  await channel.prefetch(1);

  // Ensure /data exists inside container
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  } catch {}

  let processedIds = loadProcessedIds();
  const processedSet = new Set(processedIds);

  console.log(
    `[Notification] Consuming from ${QUEUE}, fail rate: ${FAIL_RATE}%`,
  );
  console.log(`[Notification] Log file: ${NOTIFICATION_LOG_FILE}`);
  console.log(`[Notification] Processed IDs file: ${PROCESSED_IDS_FILE}`);

  channel.consume(QUEUE, async (msg) => {
    if (!msg) return;

    const orderEvent = JSON.parse(msg.content.toString());
    const correlationId =
      msg.properties.headers?.correlationId || orderEvent.correlationId;

    const retryCount = getRetryCount(msg);

    // Idempotent Receiver (file-backed)
    if (processedSet.has(correlationId)) {
      console.log(`[Notification] ↩️ Duplicate skipped: ${correlationId}`);
      channel.ack(msg);
      return;
    }

    console.log(
      `[Notification] Processing ${correlationId} (attempt ${retryCount + 1})`,
    );

    try {
      // Simulate failure
      const roll = Math.floor(Math.random() * 100) + 1;
      const ok = roll > FAIL_RATE;

      if (!ok) {
        throw new Error(
          `Notification failed (roll=${roll}, failRate=${FAIL_RATE}%)`,
        );
      }

      // Write log entry (JSON Lines)
      const logEntry = {
        correlationId,
        orderId: orderEvent.orderId,
        customerId: orderEvent.customerId,
        timestamp: new Date().toISOString(),
        message: "Order received",
      };

      appendNotificationLog(logEntry);

      // Persist processed IDs
      processedSet.add(correlationId);
      processedIds.push(correlationId);
      saveProcessedIds(processedIds);

      // Publish result event
      const resultEvent = {
        correlationId,
        status: "SENT",
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
        `[Notification] ✅ Sent ${correlationId} → logged + published to ${RESULTS_EXCHANGE}`,
      );
    } catch (err) {
      if (retryCount >= MAX_RETRIES - 1) {
        channel.publish(process.env.DLQ_EXCHANGE, "", msg.content, {
          headers: msg.properties.headers,
        });
        channel.ack(msg);
        console.log(
          `[Notification] → DLQ after ${retryCount + 1} attempts: ${err.message}`,
        );
      } else {
        channel.nack(msg, false, false);
        console.log(
          `[Notification] → Retry (attempt ${retryCount + 1}): ${err.message}`,
        );
      }
    }
  });
}

main().catch(console.error);
