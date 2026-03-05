# Practice 2: Event-Driven Messaging with RabbitMQ

**Course:** Enterprise Application Integration (EAI)
**Submission deadline:** 08.03.2026, 20:00 EET (Europe/Riga, UTC+2)
**Submission format:** Public GitHub repository URL sent by email to martins.leitass@turiba.lv
**Total points:** 25 (23 automated + 2 manual review); Passing score 60%;

---

## Learning Objectives

By completing this practice, you will:

- Implement the **Publish-Subscribe** pattern using a RabbitMQ fanout exchange
- Configure **Dead Letter Queues** with TTL-based delayed retry for failed messages
- Propagate a **Correlation Identifier** across producer, consumers, and result events
- Implement **Guaranteed Delivery** using manual acknowledgements and retry logic
- Build an **Idempotent Receiver** with file-based deduplication

---

## Business Scenario

An e-commerce company needs event-driven communication between its order processing services. When a customer places an order:

1. The **Order Service** accepts the order via HTTP, generates a unique correlation ID, and publishes an `order.placed` event to RabbitMQ
2. Three independent consumer services receive the same event simultaneously:
   - **Payment Service** validates the payment (simulated with a configurable failure rate)
   - **Inventory Service** checks stock availability (simulated with a configurable failure rate)
   - **Notification Service** logs the order for customer notification
3. When a consumer fails to process a message, the message is automatically retried after a 1-second delay
4. After 3 failed attempts, the message is sent to a Dead Letter Queue for manual inspection
5. Every message carries a correlation ID so the full lifecycle of an order can be traced
6. The Notification Service must be **idempotent** — receiving the same order event twice must not create duplicate log entries

---

## Prerequisites

Before starting, ensure you have:

- **Docker Desktop** installed and running (with Docker Compose support)
- **Node.js 18+** installed (`node --version` to verify)
- **Git** configured with a GitHub account
- **curl** or Postman for API testing
- The following ports available: `3000`, `5672`, `15672`

---

## Quick Start

### 1. Start the environment

```bash
docker-compose up --build
```

Or run detached:

```bash
docker-compose up -d --build
```

If your Docker installation uses the plugin syntax, replace docker-compose with docker compose in all commands in this README.

Wait for all services to initialize. RabbitMQ takes ~15 seconds for its health check to pass before other services start.

### 2. Verify services are running

**Order Service health check:**

```bash
curl http://localhost:3000/health
```

Expected: `{"status":"ok"}`

**RabbitMQ Management UI:**

Open [http://localhost:15672](http://localhost:15672) in your browser.

- Username: `guest`
- Password: `guest`

Navigate to the **Queues** tab — you should see all 10 queues declared and ready.

### 3. Stop the environment

```bash
docker-compose down
```

To also remove volumes (clears all persistent data):

```bash
docker-compose down -v
```

---

## What's Provided

You receive a fully configured infrastructure. **Do not modify** any of the following files unless explicitly instructed:

Unless explicitly instructed otherwise, edit only the four service server.js files and your own submission README content; treat all scaffold/infrastructure files as read-only.

| Component            | Files                                                       | Purpose                                                                                           |
| -------------------- | ----------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| Docker orchestration | `docker-compose.yml`                                        | Starts RabbitMQ + all 4 services with correct networking, environment variables, and volumes      |
| RabbitMQ topology    | `rabbitmq/definitions.json`, `rabbitmq/rabbitmq.conf`       | Pre-declares all exchanges, queues, and bindings (including retry and DLQ infrastructure)         |
| Shared helper        | `shared/rabbit.js`                                          | `connectWithRetry()` and `getRetryCount()` utilities for your services                            |
| Payment scaffold     | `payment-service/server.js`                                 | Partial implementation with connection, consume loop, and retry/DLQ logic. You complete the TODO. |
| Dockerfiles          | `*/Dockerfile`                                              | Standard Node.js 18 Alpine containers for each service                                            |
| Dependencies         | `*/package.json`                                            | npm dependencies pre-declared for each service                                                    |
| Test suite           | `test/test-runner.test.js`, `test/helpers/rabbit-helper.js` | 7 automated tests that validate your implementation                                               |
| Grading              | `grading/report.js`, `.github/workflows/grade.yml`          | Auto-grading pipeline that runs on every git push                                                 |

---

## Shared Helper API Reference

The file `shared/rabbit.js` is mounted read-only at `/app/shared/rabbit.js` inside every container. Import it in your services:

```javascript
const { connectWithRetry, getRetryCount } = require("/app/shared/rabbit");
```

### `connectWithRetry(url, retries?, delay?)`

Connects to RabbitMQ with exponential backoff retry. Handles the common startup race condition where your Node.js service starts before RabbitMQ is fully ready.

| Parameter | Type   | Default    | Description                                                  |
| --------- | ------ | ---------- | ------------------------------------------------------------ |
| `url`     | string | (required) | AMQP connection URL, e.g. `amqp://guest:guest@rabbitmq:5672` |
| `retries` | number | 10         | Maximum connection attempts                                  |
| `delay`   | number | 3000       | Initial delay in ms (doubles each attempt, capped at 30s)    |

**Returns:** `Promise<{ connection, channel }>` — an AMQP connection and a single channel ready to use.

**Example:**

```javascript
const { connection, channel } = await connectWithRetry(
  process.env.RABBITMQ_URL,
);
```

### `getRetryCount(msg)`

Parses the RabbitMQ `x-death` header to determine how many times a message has been retried via the dead-letter exchange.

| Parameter | Type   | Description                                                        |
| --------- | ------ | ------------------------------------------------------------------ |
| `msg`     | object | The AMQP message object received in a `channel.consume()` callback |

**Returns:** `number` — the total retry count. Returns `0` if the message has never been retried.

**Example:**

```javascript
const retryCount = getRetryCount(msg);
if (retryCount >= parseInt(process.env.MAX_RETRIES) - 1) {
  // Message has been retried too many times — send to DLQ
}
```

---

## Environment Variables Reference

These are set automatically by `docker-compose.yml`. Use `process.env.<NAME>` in your code.

| Variable              | Available In                     | Default                            | Description                                         |
| --------------------- | -------------------------------- | ---------------------------------- | --------------------------------------------------- |
| `RABBITMQ_URL`        | All services                     | `amqp://guest:guest@rabbitmq:5672` | Full AMQP connection string                         |
| `PORT`                | order-service                    | `3000`                             | HTTP port for Express server                        |
| `PAYMENT_FAIL_RATE`   | payment-service                  | `20`                               | Percentage (0-100) of payment validations that fail |
| `INVENTORY_FAIL_RATE` | inventory-service                | `10`                               | Percentage (0-100) of inventory checks that fail    |
| `DLQ_EXCHANGE`        | payment, inventory, notification | `orders.dlq.exchange`              | Exchange name for dead letter messages              |
| `MAX_RETRIES`         | payment, inventory, notification | `3`                                | Maximum processing attempts before sending to DLQ   |

---

## What You Must Build

You need to write code in **4 files**. Three are empty (`server.js`), one has a TODO to complete.

### A. Order Service (`order-service/server.js`)

Build an Express HTTP server that accepts orders and publishes them to RabbitMQ.

**Requirements:**

1. `GET /health` — returns HTTP 200 with `{ "status": "ok" }`
2. `POST /orders` — accepts an order, publishes it to RabbitMQ, returns the correlation ID
3. `GET /orders/:correlationId` — returns a previously submitted order, or 404 if not found

**Pseudocode:**

```
IMPORT express, uuid, connectWithRetry

orders = []    // in-memory store

CONNECT to RabbitMQ using connectWithRetry(RABBITMQ_URL)

ON POST /orders:
    correlationId = generate UUID v4
    orderId = generate short ID (e.g., "ord-" + first 8 chars of UUID)
    timestamp = current ISO timestamp

    enrichedOrder = { ...request.body, orderId, correlationId, timestamp }

    PUBLISH enrichedOrder to "orders.exchange" with:
        content: Buffer.from(JSON.stringify(enrichedOrder))
        routing key: "" (empty string — fanout ignores it but it's required)
        options: { headers: { correlationId }, contentType: "application/json" }

    STORE enrichedOrder in orders array

    RESPOND 201 { correlationId, status: "accepted" }

ON GET /orders/:correlationId:
    FIND order in orders array by correlationId
    IF found: RESPOND 200 with order
    ELSE: RESPOND 404

ON GET /health:
    RESPOND 200 { status: "ok" }

START Express server on process.env.PORT (default 3000)
```

**Key details:**

- You must `await` the RabbitMQ connection before starting the Express listener
- Use `channel.publish(exchangeName, routingKey, content, options)` to publish
- The routing key must be an empty string `''` for fanout exchanges (it is ignored but still required as a parameter)
- The message content must be a `Buffer`, not a plain object — use `Buffer.from(JSON.stringify(...))`
- The `correlationId` goes in `options.headers`, not only in the message body

---

### B. Payment Service (`payment-service/server.js`) — Complete the TODO

This service is **partially implemented**. Open the file and study the provided code.

**What's already provided:**

- RabbitMQ connection with `connectWithRetry`
- Consumer loop on `payments.queue` with `prefetch(1)`
- Message parsing, correlationId extraction, retry count tracking
- Complete retry/DLQ logic in the `catch` block (study this carefully — you will replicate it in other services)

**What you must implement** (replace the `throw new Error('Not implemented...')` line):

```
// Inside the try block:

IF Math.random() * 100 < FAIL_RATE:
    THROW new Error("Payment rejected")   // triggers the catch block → retry/DLQ

// If we reach here, payment succeeded:
ACK the message
PUBLISH result event to RESULTS_EXCHANGE with:
    content: {
        correlationId, source: "payment", status: "success",
        timestamp: new Date().toISOString(),
        details: { message: "Payment validated successfully" }
    }
    options: { headers: { correlationId }, contentType: "application/json" }
RETURN   // skip the catch block
```

**Important:** When payment succeeds, you must `ack` the message **and** publish a result event **before** the code reaches the `catch` block. Use `return` after your success logic so the catch block does not execute on success.

---

### C. Inventory Service (`inventory-service/server.js`)

Build from scratch, following the same pattern as the Payment Service. The provided payment scaffold is your reference implementation.

**Pseudocode:**

```
IMPORT connectWithRetry, getRetryCount

QUEUE = "inventory.queue"
RESULTS_EXCHANGE = "results.inventory"
MAX_RETRIES = parse int from process.env.MAX_RETRIES (default 3)
FAIL_RATE = parse int from process.env.INVENTORY_FAIL_RATE (default 10)

CONNECT to RabbitMQ using connectWithRetry(RABBITMQ_URL)
SET channel.prefetch(1)

CONSUME from QUEUE with callback(msg):
    IF msg is null: RETURN

    PARSE order from msg.content (Buffer → string → JSON)
    READ correlationId from msg.properties.headers.correlationId
    READ retryCount from getRetryCount(msg)

    TRY:
        IF Math.random() * 100 < FAIL_RATE:
            THROW error("Inventory check failed")

        // Success path:
        ACK message
        PUBLISH result event to RESULTS_EXCHANGE with correlationId in headers
        RETURN

    CATCH error:
        IF retryCount >= MAX_RETRIES - 1:
            PUBLISH original message content to DLQ_EXCHANGE (preserve original headers)
            ACK message    // remove from consumer queue
        ELSE:
            NACK(msg, false, false)    // triggers DLX → retry queue → return
```

**Key details:**

- This is structurally identical to the Payment Service — adapt the queue name, exchange name, environment variable, and log messages
- Always check `if (!msg) return;` at the start of the consume callback
- Use `channel.publish(exchange, '', content, { headers })` for result events
- Use `channel.nack(msg, false, false)` — the second `false` means "do not requeue" which triggers the dead-letter exchange

---

### D. Notification Service (`notification-service/server.js`)

Build from scratch. This service has additional complexity: **file-based idempotency tracking**.

**Pseudocode:**

```
IMPORT connectWithRetry, getRetryCount, fs

QUEUE = "notifications.queue"
RESULTS_EXCHANGE = "results.notification"
PROCESSED_IDS_FILE = "/data/processed-ids.json"
NOTIFICATION_LOG_FILE = "/data/notification.log"

// Load previously processed IDs on startup
TRY:
    processedIds = JSON.parse(fs.readFileSync(PROCESSED_IDS_FILE, "utf8"))
CATCH:
    processedIds = []    // file doesn't exist yet on first run — that's fine

CONNECT to RabbitMQ using connectWithRetry(RABBITMQ_URL)
SET channel.prefetch(1)

CONSUME from QUEUE with callback(msg):
    IF msg is null: RETURN

    PARSE order from msg.content
    READ correlationId from msg.properties.headers.correlationId
    READ retryCount from getRetryCount(msg)

    // === Idempotency check (MUST happen before any processing) ===
    IF processedIds includes correlationId:
        ACK message                      // already processed — skip silently
        LOG "Duplicate skipped: {correlationId}"
        RETURN

    TRY:
        // Build log entry
        logEntry = {
            correlationId,
            orderId: order.orderId,
            customerId: order.customerId,
            timestamp: new Date().toISOString(),
            message: "Order received"
        }

        // Append to notification log (JSON Lines format: one object per line)
        fs.appendFileSync(NOTIFICATION_LOG_FILE, JSON.stringify(logEntry) + "\n")

        // Track this correlationId as processed
        processedIds.push(correlationId)
        fs.writeFileSync(PROCESSED_IDS_FILE, JSON.stringify(processedIds))

        ACK message
        PUBLISH result event to RESULTS_EXCHANGE with correlationId in headers
        RETURN

    CATCH error:
        // Same retry/DLQ logic as payment and inventory services
        IF retryCount >= MAX_RETRIES - 1:
            PUBLISH to DLQ_EXCHANGE with original headers, then ACK
        ELSE:
            NACK(msg, false, false)
```

**Key details:**

- The notification log (`/data/notification.log`) must use **JSON Lines** format: one JSON object per line, separated by `\n`. **Not** a JSON array.
- The processed IDs file (`/data/processed-ids.json`) is a JSON array of correlation ID strings.
- On first startup, the files do not exist. Your code must handle this gracefully with a try/catch around the file read, defaulting to an empty array.
- The `/data` directory is bind-mounted from your host filesystem. Files persist across container restarts.
- The idempotency check must happen **before** any processing logic. If the correlationId is already known, just `ack` and return immediately.

---

## Message Schemas

### POST /orders Request Body

```json
{
  "customerId": "cust-123",
  "items": [
    { "productId": "prod-001", "quantity": 2, "unitPrice": 29.99 },
    { "productId": "prod-002", "quantity": 1, "unitPrice": 149.99 }
  ],
  "totalAmount": 209.97,
  "orderType": "standard"
}
```

### POST /orders Response (HTTP 201)

```json
{
  "correlationId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "accepted"
}
```

### Order Event (published to `orders.exchange`)

Message body:

```json
{
  "orderId": "ord-550e8400",
  "correlationId": "550e8400-e29b-41d4-a716-446655440000",
  "customerId": "cust-123",
  "items": [
    { "productId": "prod-001", "quantity": 2, "unitPrice": 29.99 },
    { "productId": "prod-002", "quantity": 1, "unitPrice": 149.99 }
  ],
  "totalAmount": 209.97,
  "orderType": "standard",
  "timestamp": "2026-03-01T10:00:00.000Z"
}
```

AMQP publish options:

```javascript
{
  headers: { correlationId: "550e8400-e29b-41d4-a716-446655440000" },
  contentType: "application/json"
}
```

### Result Event (published by each consumer to its `results.*` exchange)

```json
{
  "correlationId": "550e8400-e29b-41d4-a716-446655440000",
  "source": "payment",
  "status": "success",
  "timestamp": "2026-03-01T10:00:01.234Z",
  "details": {
    "message": "Payment validated successfully"
  }
}
```

AMQP publish options:

```javascript
{
  headers: { correlationId: "550e8400-e29b-41d4-a716-446655440000" },
  contentType: "application/json"
}
```

The `source` field should be `"payment"`, `"inventory"`, or `"notification"` depending on the service.

---

## RabbitMQ Topology

All exchanges, queues, and bindings are pre-configured in `rabbitmq/definitions.json`. You do **not** need to create them in your code. This section is a reference to help you understand the messaging infrastructure your code interacts with.

### Exchanges

| Exchange                 | Type   | Purpose                                                                 |
| ------------------------ | ------ | ----------------------------------------------------------------------- |
| `orders.exchange`        | fanout | Distributes order events to all 3 consumer queues                       |
| `orders.retry.exchange`  | direct | Routes failed (nacked) messages to service-specific retry queues        |
| `orders.return.exchange` | direct | Routes retried messages back to their specific consumer queue after TTL |
| `orders.dlq.exchange`    | fanout | Routes exhausted messages to the dead letter queue                      |
| `results.payment`        | fanout | Payment service publishes result events here                            |
| `results.inventory`      | fanout | Inventory service publishes result events here                          |
| `results.notification`   | fanout | Notification service publishes result events here                       |

### Queues

| Queue                       | Special Arguments                          | Purpose                                                    |
| --------------------------- | ------------------------------------------ | ---------------------------------------------------------- |
| `payments.queue`            | DLX: `orders.retry.exchange`               | Payment consumer reads from here                           |
| `inventory.queue`           | DLX: `orders.retry.exchange`               | Inventory consumer reads from here                         |
| `notifications.queue`       | DLX: `orders.retry.exchange`               | Notification consumer reads from here                      |
| `payments.retry.queue`      | TTL: 1000ms, DLX: `orders.return.exchange` | Holds failed payment messages for 1s before returning them |
| `inventory.retry.queue`     | TTL: 1000ms, DLX: `orders.return.exchange` | Holds failed inventory messages for 1s                     |
| `notifications.retry.queue` | TTL: 1000ms, DLX: `orders.return.exchange` | Holds failed notification messages for 1s                  |
| `orders.dlq`                | (none)                                     | Final destination for messages that exceeded max retries   |
| `payment.results`           | (none)                                     | Payment result events (used by test suite)                 |
| `inventory.results`         | (none)                                     | Inventory result events (used by test suite)               |
| `notification.results`      | (none)                                     | Notification result events (used by test suite)            |

### Retry Flow — Step by Step

This is the most important mechanism to understand. Here is what happens when a consumer **fails** to process a message:

```
1. Consumer receives message from payments.queue (attempt 1)
2. Processing fails → consumer calls channel.nack(msg, false, false)
3. RabbitMQ routes message to orders.retry.exchange (dead-letter exchange)
4. orders.retry.exchange (direct) routes to payments.retry.queue
5. Message sits in payments.retry.queue for 1 second (TTL)
6. After TTL expires, RabbitMQ dead-letters to orders.return.exchange
7. orders.return.exchange (direct) routes back to payments.queue only
8. Consumer receives the message again — x-death header now has count = 1
9. getRetryCount(msg) returns 1 → this is attempt 2
10. If processing fails again → repeat steps 2-9 (count increments)
11. On attempt 3: getRetryCount returns 2 (which is >= MAX_RETRIES - 1)
    → Consumer publishes message to orders.dlq.exchange
    → Consumer acks the message (removes it from payments.queue)
    → Message lands in orders.dlq
```

**Why ack + publish to DLQ instead of just nacking?** Because `nack(msg, false, false)` always sends to the retry exchange — that is what the queue's dead-letter-exchange is configured to do. To break out of the retry cycle and send to the DLQ instead, you must `ack` the message (removing it from the retry loop) and manually `publish` it to `orders.dlq.exchange`.

---

## Grading Criteria

Your submission is graded automatically when you push to GitHub. The test suite runs via GitHub Actions and produces a grade report.

| #   | Criterion                                       | Points | What the Test Does                                                                                                                                                                                                                   |
| --- | ----------------------------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 1   | Services start without errors                   | 3      | Checks Order Service `/health` returns 200, RabbitMQ management API is reachable, all 3 consumer queues have active consumers                                                                                                        |
| 2   | `POST /orders` returns valid correlation ID     | 4      | Posts an order, verifies HTTP 201 with UUID v4 `correlationId` and `status: "accepted"`. Also verifies `GET /orders/:correlationId` returns the stored order.                                                                        |
| 3   | Exchange and queue topology correct             | 3      | Verifies `orders.exchange` exists as fanout type, all 3 consumer queues have at least 1 active consumer                                                                                                                              |
| 4   | DLQ receives failed messages after retries      | 4      | Sets `PAYMENT_FAIL_RATE=100`, restarts payment-service, posts an order, waits for retry cycles, checks `orders.dlq` for a message with matching correlationId                                                                        |
| 5   | Correlation ID propagated through all consumers | 3      | Posts an order with 0% failure rate, consumes from `payment.results`, `inventory.results`, and `notification.results`. Verifies all three carry the original correlationId in headers.                                               |
| 6   | Idempotent notification                         | 4      | Posts an order, waits 3s, verifies `notification.log` has exactly 1 entry. Manually publishes the same event again with the same correlationId, waits 3s, verifies the log still has only 1 entry. Also checks `processed-ids.json`. |
| 7   | Retry logic (3 attempts before DLQ)             | 2      | Sets `PAYMENT_FAIL_RATE=100`, posts an order, waits for retries, checks `x-death` header on the DLQ message shows retry count >= 2                                                                                                   |
| 8   | Code quality and README                         | 2      | **Manual review.** Your README must contain a message flow explanation and an EAI pattern mapping table (see below).                                                                                                                 |

**Total: 25 points** (23 automated + 2 manual)

---

## Testing Your Work

### Send a test order

```bash
curl -X POST http://localhost:3000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "cust-123",
    "items": [
      { "productId": "prod-001", "quantity": 2, "unitPrice": 29.99 },
      { "productId": "prod-002", "quantity": 1, "unitPrice": 149.99 }
    ],
    "totalAmount": 209.97,
    "orderType": "standard"
  }'
```

Expected response (HTTP 201):

```json
{ "correlationId": "<uuid>", "status": "accepted" }
```

### Retrieve an order

```bash
curl http://localhost:3000/orders/<correlationId-from-previous-response>
```

### Inspect RabbitMQ

Open [http://localhost:15672](http://localhost:15672) (guest/guest) and check:

- **Queues tab** — see messages flowing through queues, check consumer counts
- **Exchanges tab** — verify `orders.exchange` has bindings to 3 queues
- **Get Messages** button on any queue — peek at message contents and headers

This is your best debugging tool. If messages are not arriving at a consumer, check whether they are sitting in the queue and whether the queue has an active consumer.

### Run the test suite locally

Make sure all services are running (`docker-compose up -d --build`), then:

```bash
cd test
npm install
npx jest --runInBand --forceExit
```

Each test prints a pass/fail result. Failed tests include an error message explaining what was expected vs. what happened.

### Generate a grade report

After running tests with JSON output:

```bash
cd test
npx jest --runInBand --forceExit --json --outputFile=results.json
cd ..
node grading/report.js test/results.json
```

This creates `grade-report.md` in the project root with your score breakdown.

---

## Submission Instructions

### Deadline

**08.03.2026, 20:00 EET (Europe/Riga, UTC+2)**

### Format

1. Create a **public GitHub repository** containing your completed project
2. Push all your code — the entire project directory including your implemented `server.js` files
3. Submit the repository URL by email to martins.leitass@turiba.lv.

### Your repository must include

- Working `order-service/server.js`
- Working `payment-service/server.js` (with TODO completed)
- Working `inventory-service/server.js`
- Working `notification-service/server.js`
- `README.md` with your own content (see below)

### README requirements (2 points)

Your README must contain the following two sections. You may extend or replace this README, but these must be present:

1. **Message flow explanation** — a written description (1-2 paragraphs) of what happens when `POST /orders` is called, tracing the message from Order Service through RabbitMQ to all three consumers, including what happens on failure.

2. **EAI Pattern mapping table** — identify where each pattern is applied and explain it in your own words:

   | Pattern                   | Where Applied | Your Explanation |
   | ------------------------- | ------------- | ---------------- |
   | Publish-Subscribe Channel |               |                  |
   | Dead Letter Channel       |               |                  |
   | Correlation Identifier    |               |                  |
   | Guaranteed Delivery       |               |                  |
   | Idempotent Receiver       |               |                  |

### Before you submit

1. Start from clean state: `docker-compose down -v`
2. Build and start: `docker-compose up --build`
3. Wait ~20 seconds for everything to initialize
4. Run tests: `cd test && npm install && npx jest --runInBand --forceExit`
5. Verify all 7 tests pass
6. Verify your README has the required sections
7. Push to GitHub and confirm the Actions workflow completes successfully

---

## Recommended Implementation Order

1. **Order Service** — start here. Get `POST /orders` working and verify that messages appear in the RabbitMQ Management UI (check `payments.queue`, `inventory.queue`, and `notifications.queue` for messages).

2. **Payment Service** — open `payment-service/server.js`, study the provided scaffold, and replace the TODO. Run the system and observe retry behavior in the RabbitMQ UI when payments fail.

3. **Inventory Service** — adapt the Payment Service pattern. Change the queue name, exchange name, environment variable, and log messages.

4. **Notification Service** — build last. This service adds file I/O and idempotency on top of the consumer pattern you have now implemented twice. Test idempotency by publishing a duplicate message via the RabbitMQ Management UI (Queues tab -> `notifications.queue` -> Publish message).

---

## Troubleshooting

### Port conflicts

If ports 3000, 5672, or 15672 are already in use:

```bash
# Windows
netstat -ano | findstr :3000

# macOS / Linux
lsof -i :3000
```

Stop the conflicting process or modify `docker-compose.yml` to use different host ports.

### Services fail to connect to RabbitMQ

The helper uses exponential backoff retries (default up to 10 attempts); delay starts at 3000 ms and doubles each attempt, capped at 30000 ms per attempt.

```bash
# Check RabbitMQ logs
docker-compose logs rabbitmq

# Restart everything from scratch
docker-compose down -v
docker-compose up --build
```

### "Module not found: /app/shared/rabbit"

The shared helper is mounted from the `shared/` directory. Make sure:

- Your Dockerfile has `WORKDIR /app` (it does by default — do not change this)
- You import with `require('/app/shared/rabbit')` (absolute path, not relative)

### Message published but consumer does not receive it

1. Open RabbitMQ Management UI -> Queues tab
2. Check if messages are sitting in the expected queue
3. Verify the queue shows `Consumers: 1` (your consumer is connected)
4. Check consumer logs: `docker-compose logs payment-service`

### Notification log format errors

The test expects **JSON Lines** format — one JSON object per line:

```
{"correlationId":"abc","orderId":"ord-1","customerId":"cust-1","timestamp":"...","message":"Order received"}
{"correlationId":"def","orderId":"ord-2","customerId":"cust-2","timestamp":"...","message":"Order received"}
```

**Not** a JSON array like `[{...}, {...}]`.

Use `fs.appendFileSync(path, JSON.stringify(entry) + '\n')`.

### Common Mistakes

| Mistake                                                                     | What Goes Wrong                                                                                     |
| --------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| Using `nack(msg, false, true)` instead of `nack(msg, false, false)`         | Message requeues immediately without retry delay. Infinite loop, no x-death header.                 |
| Forgetting to `ack` before publishing to DLQ                                | Message stays in consumer queue AND goes to DLQ.                                                    |
| Not calling `channel.prefetch(1)`                                           | Multiple messages dispatched at once, ack order breaks.                                             |
| Generating a new correlationId in consumers                                 | Each consumer gets a different ID. Read it from `msg.properties.headers.correlationId`.             |
| Publishing result events without `correlationId` in headers                 | Tests cannot trace the result back to the original order.                                           |
| Using wrong env var name (e.g., `FAIL_RATE` instead of `PAYMENT_FAIL_RATE`) | Service never fails because env var is undefined.                                                   |
| Hardcoding `amqp://localhost:5672`                                          | Works on host but fails inside Docker where hostname is `rabbitmq`. Use `process.env.RABBITMQ_URL`. |
| Writing notification.log as a JSON array                                    | Test expects JSON Lines (one object per line).                                                      |
| Not handling `msg === null` in consume callback                             | Consumer crashes when channel is cancelled. Always check `if (!msg) return;` first.                 |
| Starting Express listener before RabbitMQ connects                          | HTTP requests arrive before channel exists. Await the connection first.                             |

## Message Flow Explanation

When a client sends a POST request to `/orders`, the Order Service receives the order through the Express API. The service generates an orderId and a correlationId and publishes the event to RabbitMQ using the `orders.exchange` fanout exchange. The correlationId is included in the message headers so that all services can trace the same order through the system.

Because the exchange type is fanout, RabbitMQ automatically distributes the same message to three queues: `payments.queue`, `inventory.queue`, and `notifications.queue`. Each service consumes the message independently and processes it. Payment Service validates the payment, Inventory Service reserves the products, and Notification Service logs the order for customer notification.

If processing fails in any consumer, the message is rejected (`nack`) and sent to a retry queue using the dead-letter exchange. After a 1-second delay the message returns to the original queue and processing is attempted again. If the maximum retry count is reached, the consumer publishes the message to the `orders.dlq.exchange`, and it is stored in the `orders.dlq` queue for manual inspection.

## EAI Pattern Mapping

| Pattern                   | Where Applied                                | Explanation                                                                                                                                                                  |
| ------------------------- | -------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Publish-Subscribe Channel | RabbitMQ `orders.exchange` fanout exchange   | When Order Service publishes an order event, RabbitMQ distributes the same message to multiple consumer queues so that several services can process the order independently. |
| Dead Letter Channel       | `orders.dlq.exchange` and `orders.dlq` queue | Messages that fail processing multiple times are sent to the Dead Letter Queue, allowing failed events to be inspected or reprocessed later.                                 |
| Correlation Identifier    | `correlationId` header in RabbitMQ messages  | A unique correlationId is generated by the Order Service and passed through all services, allowing the lifecycle of an order to be traced across multiple systems.           |
| Guaranteed Delivery       | Manual `ack`/`nack` with retry queues        | Messages are acknowledged only after successful processing. If processing fails, the message is retried using TTL-based retry queues to ensure it is not lost.               |
| Idempotent Receiver       | Notification Service duplicate check         | Notification Service keeps track of processed correlationIds and ignores duplicate messages to avoid creating duplicate log entries.                                         |
