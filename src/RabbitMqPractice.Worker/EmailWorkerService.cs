using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMqPractice.Messaging;
using RabbitMqPractice.Messaging.Events;
using RabbitMqPractice.Messaging.Names;

namespace RabbitMqPractice.Worker
{
    public sealed class EmailWorkerService(ConfigureConnectionFactory connectionFactory) : BackgroundService
    {
        private const string _serviceName = "email";
        private readonly SemaphoreSlim _channelSemaphore = new(1, 1);

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            ConnectionFactory factory = connectionFactory.CreateConnectionFactory();
            factory.ClientProvidedName = _serviceName;

            await using IConnection connection = await factory.CreateConnectionAsync(cancellationToken: stoppingToken);
            await using IChannel channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);

            await Topology.DeclareStoreEventsAsync(channel, stoppingToken);
            await Topology.DeclarePurchaseEmailQueueAsync(channel, stoppingToken);
            await Topology.DeclarePurchaseEmailDeadQueueAsync(channel, stoppingToken);

            await channel.BasicQosAsync(
                prefetchSize: 0,
                prefetchCount: 1,
                global: false,
                cancellationToken: stoppingToken);

            Console.WriteLine($"[{_serviceName}] [INFO ] Waiting for any purchase messages to email.");

            AsyncEventingBasicConsumer consumer = new(channel);
            consumer.ReceivedAsync += async (model, ea) =>
            {
                await _channelSemaphore.WaitAsync();

                try
                {
                    if (stoppingToken.IsCancellationRequested)
                    {
                        Console.WriteLine($"[{_serviceName}] [WARN ] Shutdown in progress; emails left for redelivery.");
                        return;
                    }

                    Guid? messageId = null;
                    MessageOutcome outcome = MessageOutcome.Park;

                    byte[] body = ea.Body.ToArray();
                    PurchaseCompleted purchaseCompleted;

                    try
                    {
                        purchaseCompleted = EventSerializer.Deserialize<PurchaseCompleted>(body);
                        messageId = purchaseCompleted.MessageId;
                        outcome = MessageOutcome.Ack;
                        Console.WriteLine($"[{_serviceName}] [DEBUG] Received PurchaseCompleted message '{messageId}'");
                    }
                    catch (Exception ex)
                    {
                        outcome = MessageOutcome.Park;
                        Console.WriteLine($"[{_serviceName}] [ERROR] Exception while reading the message: {ex.GetType().Name}: {ex.Message}.");
                    }

                    await Task.Delay(1000); // Simulate processing time

                    switch (outcome)
                    {
                        case MessageOutcome.Ack:
                            await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
                            Console.WriteLine($"[{_serviceName}] [INFO ] Processing of message '{messageId}' succeeded.");

                            break;
                        case MessageOutcome.Park:
                            BasicProperties properties = new() { Persistent = true, Headers = ea.BasicProperties.Headers };

                            await channel.BasicPublishAsync(
                                exchange: ExchangeName.DeadLetter,
                                routingKey: RoutingKeyName.PurchaseEmailDead,
                                mandatory: false,
                                basicProperties: properties,
                                body: body);

                            await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);

                            break;
                        default:
                            throw new InvalidOperationException($"[{_serviceName}] Unhandled message outcome: {outcome}");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{_serviceName}] [ERROR] Exception during message processing: {ex.GetType().Name}: {ex.Message}. " +
                        "The message was not acknowledged.");

                    throw;
                }
                finally
                {
                    _channelSemaphore.Release();
                }
            };

            string consumerTag = await channel.BasicConsumeAsync(
                queue: QueueName.PurchaseEmail,
                autoAck: false,
                consumer: consumer,
                cancellationToken: stoppingToken);

            try
            {
                await Task.Delay(Timeout.Infinite, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                Console.WriteLine($"[{_serviceName}] [WARN ] Cancellation requested; stopping service.");
            }

            try
            {
                using CancellationTokenSource cts = new(TimeSpan.FromSeconds(3));
                CancellationToken cleanupToken = cts.Token;

                await channel.BasicCancelAsync(consumerTag, cancellationToken: cleanupToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{_serviceName}] [ERROR] Canceling the consumer failed: {ex.GetType().Name}: {ex.Message}");
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
                    Console.WriteLine($"[{_serviceName}] [WARN ] Handler still running after {waitSeconds} seconds; its message will be redelivered.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{_serviceName}] [ERROR] Waiting for the gate failed: {ex.GetType().Name}: {ex.Message}");
            }

            try
            {
                using CancellationTokenSource cts = new(TimeSpan.FromSeconds(5));
                CancellationToken cleanupToken = cts.Token;

                await channel.CloseAsync(cleanupToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{_serviceName}] [ERROR] Closing the channel failed: {ex.GetType().Name}: {ex.Message}");
            }

            try
            {
                using CancellationTokenSource cts = new(TimeSpan.FromSeconds(5));
                CancellationToken cleanupToken = cts.Token;

                await connection.CloseAsync(cleanupToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{_serviceName}] [ERROR] Closing the connection failed: {ex.GetType().Name}: {ex.Message}");
            }
        }
    }
}
