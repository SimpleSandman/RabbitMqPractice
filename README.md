# RabbitMQ Practice Project

A hands-on practice project for learning message-based systems with RabbitMQ and .NET 10. It is not production code, and it is not a claim of production message-bus experience. The goal was to understand the moving parts (acknowledgements, durability, retries, dead-lettering, routing) by building them and watching them fail.

Everything runs locally: RabbitMQ in Docker Compose, plus a console Sender, an ASP.NET Core API, a Worker Service with three consumers, and a small shared library. They use the official `RabbitMQ.Client` library directly (no MassTransit or other framework), so the concepts stay visible.

## At a glance

- Two flows on one broker: a practice flow (Sender to Worker) and a store flow (API to a topic exchange to two independent consumers).
- Manual acknowledgements, delayed retries through a TTL wait queue, dead-letter queues, and a shutdown that finishes the message in progress (within a 15 second budget).
- 35 automated xUnit test cases on the retry logic and the event serializer, plus broker behaviors verified by hand (see [Testing](docs/testing.md)).
- What is missing is listed in [Known limitations](docs/known-limitations.md) and under "Not included" below.

## What it does

The project has two flows that share one broker.

**The practice flow.** A **Sender** publishes a text message to a durable queue. The Worker's practice service consumes it with manual acknowledgements and a prefetch of 1. "Processing" is simulated: it takes one second by default (see [Configuration](docs/running.md#configuration)), and any message containing the word `poison` (case-insensitive) is treated as a failure.

A failed message is not retried immediately. It goes to a wait queue, sits there for 5 seconds, and returns to the work queue. After 2 retries it is moved to a dead-letter queue so it can be inspected.

**The store flow.** An **API** accepts a purchase (`POST /purchases` with a product name and a quantity), validates it, and publishes a `PurchaseCompleted` event to a topic exchange. The Worker's store service reads the events from its own queue. The API publishes to an exchange, not to a queue, so it does not know who consumes the events. The consumer does no real work: it reads the event, prints its ID, and acknowledges it. Processing is simulated, and a product name containing `poison` counts as a transient failure. As in the practice flow, a failed event goes to a wait queue, returns after 5 seconds, and is parked in its own dead-letter queue after 2 retries.

A second consumer, the Worker's email service, reads the same events from its own queue. It is a stub: it prints the event ID and acknowledges it, and it never retries. Each consumer gets its own copy of every event.

## Prerequisites

- Docker Desktop
- .NET 10 SDK
- PowerShell, only for the topology script in `scripts`

Versions used while building this: RabbitMQ 4.3.6 (the floating `rabbitmq:4-management` tag resolved to this), `RabbitMQ.Client` 7.2.2, `Microsoft.Extensions.Hosting` 10.0.13, `Microsoft.AspNetCore.OpenApi` 10.0.13, xunit.v3 4.0.1. Every package version is listed once, in `Directory.Packages.props`.

## Quick start

```bash
docker compose up -d
dotnet user-secrets set "RabbitMQ:UserName" "admin" --project src/RabbitMqPractice.Worker
dotnet user-secrets set "RabbitMQ:Password" "password" --project src/RabbitMqPractice.Worker
dotnet run --project src/RabbitMqPractice.Worker
```

The management UI is at http://localhost:15672 (user `admin`, password `password`, throwaway local credentials). In a second terminal, send a message that fails, retries twice, and ends up in the dead-letter queue:

```bash
dotnet run --project src/RabbitMqPractice.Sender -- poison B
```

The API, the configuration, stopping, and troubleshooting are in [How to run](docs/running.md).

## Documentation

| Document | What is in it |
|---|---|
| [Architecture](docs/architecture.md) | Both flows with diagrams and tables, the message flows step by step, and how a Worker service is structured |
| [How to run](docs/running.md) | Secrets, configuration, the store flow demos, stopping and resetting, the topology script, and troubleshooting |
| [Testing](docs/testing.md) | The automated tests, and what was verified by hand against a real broker |
| [Design decisions](docs/design-decisions.md) | What I chose, and why |
| [Known limitations](docs/known-limitations.md) | What I did not test, and what is missing |
| [Gotchas](docs/gotchas.md) | The mistakes I made, and how I found them |

## Not included

- Publisher confirms
- An idempotent handler or duplicate-message detection
- Authentication on the API
- A Dockerfile for the apps
- Dependency injection for the connections, channels, and topology objects (each Worker service owns its own, and the API registers its publisher)
- MassTransit or any other messaging framework
- Integration tests against a real broker, including shutdown behavior, and API tests with a fake publisher
- Quorum queues, clustering, or multi-node behavior

## Repository layout

```text
Directory.Build.props                      Target framework, nullable, and implicit usings for every project
Directory.Packages.props                   Central package versions
compose.yaml                               RabbitMQ with the management UI
docs/                                      Architecture, how to run, testing, design decisions, known limitations, and gotchas
global.json                                Selects the Microsoft.Testing.Platform test runner
scripts/topology-snapshot.ps1              Saves or compares a snapshot of the broker's exchanges, queues, and bindings
src/RabbitMqPractice.Api/                  Minimal API: POST /purchases publishes PurchaseCompleted events to store.events
src/RabbitMqPractice.Messaging/            Names, the topology classes (a base and one per flow), the PurchaseCompleted event and its serializer, connection settings from configuration
src/RabbitMqPractice.Sender/               Publishes a practice message from the command line
src/RabbitMqPractice.Worker/               Worker Service with three consumers in Practice/, Store/, and Email/, plus Common/ (the consumer base class, ConsumerResult, and RetryPolicy)
tests/RabbitMqPractice.Messaging.Tests/    xUnit v3 tests for the event serializer
tests/RabbitMqPractice.Worker.Tests/       xUnit v3 tests for RetryPolicy
```