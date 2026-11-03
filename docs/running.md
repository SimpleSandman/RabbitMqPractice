# How to run

[README](../README.md) | [Architecture](architecture.md) | [How to run](running.md) | [Testing](testing.md) | [Design decisions](design-decisions.md) | [Known limitations](known-limitations.md) | [Gotchas](gotchas.md)

```bash
docker compose up -d
```

Starts RabbitMQ with the management UI at http://localhost:15672 (user `admin`, password `password`). These are throwaway local credentials. Ports are bound to `127.0.0.1` only, and the container is capped at 2 CPUs and 2 GB of memory.

Next, give the apps the same credentials. The broker host (`localhost`) is in each app's `appsettings.json`. The user name and password are .NET user secrets, which are stored outside the repository and are loaded only in the Development environment. All three apps' launch profiles set Development, so `dotnet run` picks them up. All three projects share one user secrets ID, so you set the values once:

```bash
dotnet user-secrets set "RabbitMQ:UserName" "admin" --project src/RabbitMqPractice.Worker
dotnet user-secrets set "RabbitMQ:Password" "password" --project src/RabbitMqPractice.Worker
```

The values must match `RABBITMQ_DEFAULT_USER` and `RABBITMQ_DEFAULT_PASS` in `compose.yaml`. No `user-secrets init` is needed, because the secrets ID is already in the project files. If a value is missing, the connection code throws an `InvalidOperationException` such as `UserName is not configured`.

In one terminal, start the Worker. Its three services declare the exchanges and queues of both flows:

```bash
dotnet run --project src/RabbitMqPractice.Worker
```

Press Ctrl+C to stop it. [Step 6 of the practice message flow](architecture.md#message-flow-the-practice-flow) describes what happens to a message in progress.

## Configuration

The Worker's `appsettings.json` has one setting besides the broker host:

```json
"Simulation": { "ProcessingDelayMilliseconds": 1000 }
```

It is the simulated processing time for every message. To change it for one run without editing a file, set an environment variable. For example, in PowerShell:

```powershell
$env:Simulation__ProcessingDelayMilliseconds = "10000"
dotnet run --project src/RabbitMqPractice.Worker
Remove-Item Env:Simulation__ProcessingDelayMilliseconds
```

The setting has no default in code, so a missing value reads as 0 (see [Known limitations](known-limitations.md)).

## The practice flow

In another terminal, send messages. The text comes from the command line, and defaults to `ticket A` when omitted:

```bash
dotnet run --project src/RabbitMqPractice.Sender -- ticket A
dotnet run --project src/RabbitMqPractice.Sender -- poison B
```

`ticket A` is processed and acknowledged. `poison B` fails, retries twice, and ends up in `practice-dead`, which you can confirm in the management UI.

## The store flow

Start the Worker first, at least once. The API declares only the exchange, and the store service declares its own queue, so an event published before that queue has ever existed is dropped by the broker.

Then start the API:

```bash
dotnet run --project src/RabbitMqPractice.Api
```

This uses the `http` launch profile, so the API listens on http://localhost:5025. The Visual Studio debugger defaults to the `https` profile on port 7261. With the `http` profile, a warning that the HTTPS port could not be determined appeared in every run, and it was harmless.

Send requests with the `.http` file in `src/RabbitMqPractice.Api`, which Visual Studio can run. It has a valid purchase and several invalid ones. A valid request returns 202 with the event, and the Worker prints `Received PurchaseCompleted message '<id>'` and `Processing of message '<id>' succeeded.` The email service prints the same ID, tagged `[email]`.

To watch a retry, send a request whose product name contains `poison`, such as `poison widget`. The Worker receives the event three times, then parks it in `purchase-dead`, where the management UI shows its `x-death` header.

To see the wildcard, publish a valid `PurchaseCompleted` body from the management UI to `store.events` with the key `purchase.refunded`. Only the email service receives it.

To see an event without the consumers, create a queue in the management UI, bind it to `store.events` with the key `purchase.completed`, and use Get Messages on it.

## Stopping and resetting

Stop everything and wipe all broker state, including the data volume:

```bash
docker compose down -v
```

Plain `docker compose down` keeps the volume, so durable queues and persistent messages survive.

## Checking the topology

`scripts/topology-snapshot.ps1` saves or compares a snapshot of the broker's exchanges, queues, and bindings. It leaves out message counts and sorts each list, so only a real change in a name, an argument, or a binding shows up. Use it to prove that a refactor changed nothing on the broker:

```powershell
docker compose down -v
docker compose up -d
dotnet run --project src/RabbitMqPractice.Worker      # let it start, then press Ctrl+C
.\scripts\topology-snapshot.ps1 -Mode Save            # writes a baseline to $HOME\rmq-baseline
```

After a change, repeat the wipe and the Worker start, then run `.\scripts\topology-snapshot.ps1`. It prints `MATCH` or `DIFFERENT` for each of the three files, and exits with code 1 if anything differs.

## Troubleshooting

- On one Windows machine, `docker compose up` failed with an error about `docker-credential-desktop` not being found. Running the same command from the Docker Desktop terminal worked. I did not diagnose the cause.
- `UserName is not configured` (or the same message for another setting) means that setting was not found. The user secrets may not be set, or the app may not be running in the Development environment. For example, `dotnet run --no-launch-profile` skips the launch profile, so the secrets are not loaded.
- A connection that just appeared shows 0 channels in the management UI. The statistics lag by a few seconds, and a refresh showed the channel (observed after starting the Worker).
- The API returns 202 but the Worker never prints the event. The store service's queue probably did not exist yet when the event was published, so the broker dropped it. Start the Worker first.
