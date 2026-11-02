using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMqPractice.Messaging;
using RabbitMqPractice.Messaging.Names;
using RabbitMqPractice.Messaging.Topology;

namespace RabbitMqPractice.Worker.Common
{
    public abstract class ConsumerServiceBase(ConfigureConnectionFactory connectionFactory, TopologyBase topology, IConfiguration configuration) 
        : BackgroundService
    {
        private readonly SemaphoreSlim _channelSemaphore = new(1, 1);
        private readonly int _processingDelayMilliseconds = configuration.GetValue<int>("Simulation:ProcessingDelayMilliseconds");

        protected abstract string ServiceName { get; }
        protected abstract string ConsumeQueue { get; }
        protected abstract string ParkRoutingKey { get; }
        protected abstract string WaitingMessage { get; }
        protected virtual bool SupportsRetry => false;
        protected abstract Task<ConsumerResult> HandleAsync(BasicDeliverEventArgs ea, CancellationToken stoppingToken);

        protected void Log(string level, string message)
        {
            if (level.Length < 5)
            {
                level = level.PadRight(5);
            }

            string serviceName = ServiceName;
            if (serviceName.Length < 8)
            {
                serviceName = serviceName.PadRight(8);
            }

            Console.WriteLine($"[{serviceName}] [{level}] {message}");
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            ConnectionFactory factory = connectionFactory.CreateConnectionFactory();
            factory.ClientProvidedName = ServiceName;

            await using IConnection connection = await factory.CreateConnectionAsync(cancellationToken: stoppingToken);
            await using IChannel channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);

            await topology.DeclareAsync(channel, stoppingToken);

            await channel.BasicQosAsync(
                prefetchSize: 0,
                prefetchCount: 1,
                global: false,
                cancellationToken: stoppingToken);

            Log("INFO", WaitingMessage);

            AsyncEventingBasicConsumer consumer = new(channel);
            consumer.ReceivedAsync += async (model, ea) =>
            {
                await _channelSemaphore.WaitAsync();

                try 
                { 
                    if (stoppingToken.IsCancellationRequested)
                    {
                        Log("WARN", "Shutdown in progress; message left for redelivery.");
                        return;
                    }

                    ConsumerResult result = await HandleAsync(ea, stoppingToken);

                    // Simulate some processing time.
                    // No token on purpose as we want to "drain" instead of "abandon".
                    await Task.Delay(_processingDelayMilliseconds);

                    switch (result.Outcome)
                    {
                        case MessageOutcome.Ack:
                            await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
                            Log("INFO", $"Processing of message '{result.MessageLabel}' succeeded.");

                            break;
                        case MessageOutcome.Retry:
                            if (!SupportsRetry)
                            {
                                throw new InvalidOperationException($"[{ServiceName}] Retries are not supported.");
                            }

                            await channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
                            Log("WARN", $"Processing of message '{result.MessageLabel}' failed; will retry.");

                            break;
                        case MessageOutcome.Park:
                            Log("ERROR", $"Processing of message '{result.MessageLabel}' failed.");

                            BasicProperties properties = new() { Persistent = true, Headers = ea.BasicProperties.Headers };
                            await channel.BasicPublishAsync(
                                exchange: ExchangeName.DeadLetter,
                                routingKey: ParkRoutingKey,
                                mandatory: false,
                                basicProperties: properties,
                                body: ea.Body);

                            await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
                            break;
                        default:
                            throw new InvalidOperationException($"[{ServiceName}] Unexpected message outcome: {result.Outcome}");
                    }
                }
                catch (Exception ex)
                {
                    Log("ERROR", $"Exception during message processing: {ex.GetType().Name}: {ex.Message}. " +
                        "The message was not acknowledged.");

                    throw;
                }
                finally
                {
                    _channelSemaphore.Release();
                }
            };

            string consumerTag = await channel.BasicConsumeAsync(
                queue: ConsumeQueue,
                autoAck: false,
                consumer: consumer,
                cancellationToken: stoppingToken);

            try
            {
                await Task.Delay(Timeout.Infinite, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                Log("WARN", "Cancellation requested; stopping service.");
            }

            try
            {
                using CancellationTokenSource cts = new(TimeSpan.FromSeconds(3));
                CancellationToken cleanupToken = cts.Token;

                await channel.BasicCancelAsync(consumerTag, cancellationToken: cleanupToken);
            }
            catch (Exception ex)
            {
                Log("ERROR", $"Canceling the consumer failed: {ex.GetType().Name}: {ex.Message}");
            }

            try
            {
                using CancellationTokenSource cts = new(TimeSpan.FromSeconds(20));
                CancellationToken cleanupToken = cts.Token;

                const int waitSeconds = 15;
                if (await _channelSemaphore.WaitAsync(TimeSpan.FromSeconds(waitSeconds), cleanupToken))
                {
                    _channelSemaphore.Release();
                }
                else
                {
                    Log("WARN", $"Handler still running after {waitSeconds} seconds; its message will be redelivered.");
                }
            }
            catch (Exception ex)
            {
                Log("ERROR", $"Waiting for the gate failed: {ex.GetType().Name}: {ex.Message}");
            }

            try
            {
                using CancellationTokenSource cts = new(TimeSpan.FromSeconds(5));
                CancellationToken cleanupToken = cts.Token;

                await channel.CloseAsync(cleanupToken);
            }
            catch (Exception ex)
            {
                Log("ERROR", $"Closing the channel failed: {ex.GetType().Name}: {ex.Message}");
            }

            try
            {
                using CancellationTokenSource cts = new(TimeSpan.FromSeconds(5));
                CancellationToken cleanupToken = cts.Token;

                await connection.CloseAsync(cleanupToken);
            }
            catch (Exception ex)
            {
                Log("ERROR", $"Closing the connection failed: {ex.GetType().Name}: {ex.Message}");
            }
        }
    }
}
