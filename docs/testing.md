# What is tested

[README](../README.md) | [Architecture](architecture.md) | [How to run](running.md) | [Testing](testing.md) | [Design decisions](design-decisions.md) | [Known limitations](known-limitations.md) | [Gotchas](gotchas.md)

## Automated (35 xUnit v3 test cases)

`dotnet test` from the repo root runs both test projects.

**`RabbitMqPractice.Worker.Tests` (17 cases)** cover the pure decision logic, which lives in `RetryPolicy`:

- `Decide`: success always acknowledges, failures retry below the limit and park at or above it, including the boundary and a limit of 0.
- `GetDeathCount`: returns 0 for null headers or headers without `x-death`; reads the first entry's count (integer and long); throws `InvalidOperationException` for an empty array, a wrong-typed `x-death`, a non-dictionary entry, a missing `count`, a non-numeric count, and a null count.

I also checked that these tests can fail by temporarily introducing three bugs (`>=` changed to `>`, a wrong-typed header returning 0, and a null-unsafe count parse). Each was caught by exactly the tests expected.

**`RabbitMqPractice.Messaging.Tests` (18 cases)** cover the event serializer:

- A serialized event deserializes to an equal event.
- The JSON uses camelCase names: `messageId`, `timestamp`, `quantity`, and `productName`.
- Valid bodies deserialize: camelCase names, PascalCase names, and a body with an unknown extra property.
- Invalid bodies throw `JsonException`: an empty body, truncated JSON, the `null` literal, an array instead of an object, an empty object, a quantity that is not a number, and a missing quantity.
- Three standalone tests: missing required properties (the message mentions "required"), malformed JSON, and the `null` literal (the message mentions "null").

The valid bodies act as a control. A deserializer that rejected everything would fail them, so the invalid rows cannot pass by accident.

Nothing automated covers the API endpoint, the publisher, any of the three Worker services, the topology classes, or the host.

## Verified by hand against a real broker

Nothing automated talks to RabbitMQ. These behaviors were observed manually. Rows marked (v1.0.0) were checked on the original console version, before the Worker Service host, and were not repeated. Some checks used a temporary longer simulated delay (5, 10, or 30 seconds) so the window was easy to hit. At first it was a code edit, and later the `Simulation` setting.

| Behavior | How it was checked |
|---|---|
| A message is removed only when acknowledged | Fetched messages in the management UI with different ack modes; stopped a consumer mid-message and watched the broker redeliver it (v1.0.0) |
| Durable queue plus persistent message survives a broker restart; a non-persistent message does not | `docker compose restart` with each kind of message (v1.0.0) |
| Data survives `down` but not `down -v` | Created a durable queue, then compared the broker's startup logs after each command (v1.0.0) |
| Prefetch of 1 stops one worker holding a batch of messages | Compared queue counts with and without it; ran two consumers against one queue (v1.0.0) |
| Retry with delay, retry cap, and dead-lettering | Ran `poison` messages end to end |
| The full topology comes up from code alone | Wiped the broker with `docker compose down -v`, started the Worker, and repeated the poison test |
| Starting the Sender first or the Worker first gives the same broker state | Wiped the broker, ran each order, and compared `rabbitmqctl list_queues name durable arguments messages` with a saved baseline. The output was identical both times. |
| A message in progress at Ctrl+C is finished and acknowledged | With a 10 second simulated delay, pressed Ctrl+C mid-message. The message was acknowledged and the queue was empty afterward. |
| No new message is started after Ctrl+C | Queued two messages and pressed Ctrl+C during the first. The second stayed in the queue and was processed on the next start. |
| A handler that outlives the 15 second wait loses its acknowledgement | With a 30 second simulated delay, the Worker warned after about 15 seconds. The acknowledgement failed with `AlreadyClosedException`, which was logged, and the message returned to the queue. Shutdown took about 29 seconds. |
| The Worker loads user secrets through the default host | Ran it with `dotnet run`. It reported the Development environment and connected, with the user name and password coming only from user secrets. |
| A missing setting is reported by name | Ran the Sender with `dotnet run --no-launch-profile`, so no secrets were loaded. It threw `InvalidOperationException: UserName is not configured`. |
| The API validates requests | Sent a valid request (202 with the event), a quantity of 0, an empty name, a null name, and a body with no quantity. Every invalid request returned 400. Validation failures returned a JSON body with the field errors. The missing quantity failed earlier, during binding, and returned a plain-text exception message instead. A body that tried to choose its own message ID got a server-generated one back. |
| A published event reaches a bound queue in the right shape | Bound a scratch queue to `store.events` by hand with key `purchase.completed`, sent a request, and used Get Messages. The message had the right exchange and routing key, delivery mode 2 (persistent), content type `application/json`, and a camelCase JSON body. |
| An event published to an exchange with no bound queue is dropped without an error | Removed the binding and sent a request. The API still returned 202, and the queue's message count did not change. With the binding restored, the next request arrived. |
| An event published before the consumer's queue exists is lost | Wiped the broker, started only the API, sent a request, then started the Worker. The Worker received nothing. |
| `store.events` is a durable topic exchange | `rabbitmqctl list_exchanges name type durable` |
| The store service reads events from its own queue | Wiped the broker, then ran the API first and the Worker second, and the Worker first and the API second. Each time the Worker printed the event ID and acknowledged it, and `purchase-processing` was empty afterward. |
| Unreadable purchase messages are parked in their own queue | Published `not json` and `{"quantity":1}` to `store.events` from the management UI. Each was logged with the serializer's exception and parked. `purchase-dead` went from 0 to 2, and `practice-dead` stayed at 1. |
| A transient failure on the store queue is retried with a delay, then parked | Sent a request with the product name `poison widget`. The Worker received the event three times, logging "retried 1 time(s)" and "retried 2 time(s)" on the second and third, then parked it. It sat in `purchase-dead` with an `x-death` header. This run used a 5 second simulated delay. |
| A retry wait does not block the consumer | Sent a normal request right after the failing event's first rejection. The Worker processed it before the failing event's second delivery. This run used a 5 second simulated delay. |
| An unreadable body skips the retry loop | Published `not json` from the management UI. It was logged and parked at once, with no retry lines, and the parked message had no `x-death` header. |
| The `x-death` header on the purchase queue has the same shape as on the practice queue | Read in the management UI on the parked message: the wait queue's `expired` entry first, then the work queue's `rejected` entry, both with count 2. The `rejected` entry named `store.events` and the key `purchase.completed`. RabbitMQ also added `x-first-death-*` and `x-last-death-*` headers. The `time` values were five seconds apart, and by the console timing they were the first occurrence, not the latest. That last part is my reading of one run. |
| One event reaches two consumers, each with its own copy | Sent one request with the Worker running: the store service and the email service printed the same message ID and both succeeded. With the Worker stopped, three requests left 3 messages in `purchase-processing` and 3 in `purchase-email`. Starting the Worker drained both, and each service processed the same three IDs in the same order. |
| A wildcard binding routes in code | Published a valid event from the management UI to `store.events` with the key `purchase.refunded`. Only the email service received it, and `purchase-processing` stayed at 0. |
| Each consumer parks unreadable messages in its own dead-letter queue | Published `not json` with the key `purchase.completed`. Each service logged the read failure and parked its copy: `purchase-dead` and `purchase-email-dead` each went up by 1, and `practice-dead` did not change. |
| Consumers fail independently | Sent a request with the product name `poison widget`. The store service retried it twice and then parked it in `purchase-dead`. The email service, which has no failure rule, acknowledged it once, and `purchase-email-dead` did not change. |
| The practice flow is unchanged by the store flow | After the store topology was added, again after the purchase retry topology, and again after the email queues, the `rabbitmqctl list_queues name durable arguments messages` output for the three practice queues was identical to the saved baseline. |
| The API's connection is named and closes cleanly | The `ClientProvidedName` appeared in the broker log and the management UI. Every API shutdown logged a matching `closing AMQP connection` line at info level, with no warning. |
| Each service has its own named connection | The management UI's Connections tab showed four connections named `practice`, `store`, `email`, and `api`, with one channel each. A look right after the Worker started showed 0 channels on the Worker connections, and a refresh a few seconds later showed 1 each, so the UI's statistics lag. |
| The topology script detects a change | Saved a baseline from `v1.3.0` and compared on the unchanged broker (three `MATCH` lines). Then added a throwaway queue in the management UI. The comparison reported the queue in the queues file and its implicit default-exchange binding in the bindings file. |
| The refactor left the topology unchanged | After the shared build settings and central package versions, the Worker folder and namespace move, the topology classes, and the consumer base class, the snapshot after a wipe and a Worker start matched the baseline each time. |
| The topology classes redeclare against an existing broker | Started the refactored Worker against a broker that the previous code had declared, without wiping it. There was no `PRECONDITION_FAILED`, so no queue argument changed. |
| The API and the Sender declare only their own part | After a wipe, starting only the API left `store.events` as the only non-default exchange. Running only the Sender left one queue, `practice-queue`, with its three arguments unchanged. |
| The flows behave the same after the consumer base class | Sent `ticket A` and `poison B` from the Sender, then from the API a valid request and `poison widget`, and from the management UI `not json` (key `purchase.completed`) and a valid body (key `purchase.refunded`). The final counts were `practice-dead` 1, `purchase-dead` 2, `purchase-email-dead` 1, and every other queue 0, the same as before the extraction. |
| The delay setting overrides without a code edit | Set `Simulation__ProcessingDelayMilliseconds` to 10000 for one run, pressed Ctrl+C mid-message, and both in-flight handlers finished and acknowledged. Without it, messages took about a second again. |
| Shutdown still drains after the base class | Repeated the mid-message Ctrl+C on the store service (it finished and acknowledged) and the two-queued-messages check (the first finished, and the second was processed on the next start). |
| The host stops the services one after another by default, and at once with `ServicesStopConcurrently` | With the default, after Ctrl+C the store service's stopping line printed first, and the practice and email lines only after its 10 second drain finished. That is the reverse of the registration order. With `ServicesStopConcurrently = true`, all three stopping lines appeared right away, both in-flight messages were still finished and acknowledged, and every queue was empty afterward. |
| Topic exchange routing with several wildcard bindings | **Partly a UI exercise.** Built an exchange with three wildcard bindings in the management UI and published to it by hand. The code binds one queue with an exact key (`purchase.completed`) and one with a wildcard (`purchase.*`), and the rows above check that routing. |
