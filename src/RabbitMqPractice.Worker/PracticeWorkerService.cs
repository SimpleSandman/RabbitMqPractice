using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMqPractice.Messaging;
using RabbitMqPractice.Messaging.Names;
using System.Text;

namespace RabbitMqPractice.Worker
{
    public sealed class PracticeWorkerService(ConfigureConnectionFactory connectionFactory) : BackgroundService
    {
        private readonly SemaphoreSlim _channelSemaphore = new(1, 1);

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            ConnectionFactory factory = connectionFactory.CreateConnectionFactory();
            factory.ClientProvidedName = "practice";

            await using IConnection connection = await factory.CreateConnectionAsync(cancellationToken: stoppingToken);
            await using IChannel channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);

            #region Exchange and Queue Setup

            await Topology.DeclareRetryDeadLetterAsync(channel, stoppingToken);
            await Topology.DeclareWorkQueueAsync(channel, stoppingToken);

            await channel.BasicQosAsync(
                prefetchSize: 0,
                prefetchCount: 1,
                global: false,
                cancellationToken: stoppingToken);

            #endregion

            const long maxRetries = 2L;

            Console.WriteLine(" [INFO ] Waiting for messages.");

            AsyncEventingBasicConsumer consumer = new(channel);
            consumer.ReceivedAsync += async (model, ea) =>
            {
                await _channelSemaphore.WaitAsync();

                try
                {
                    if (stoppingToken.IsCancellationRequested)
                    {
                        Console.WriteLine(" [WARN ] Shutdown in progress; message left for redelivery.");
                        return;
                    }

                    byte[] body = ea.Body.ToArray();
                    string message = Encoding.UTF8.GetString(body);
                    Console.WriteLine($" [DEBUG] Received '{message}'");

                    long deathCount = RetryPolicy.GetDeathCount(ea.BasicProperties.Headers);
                    if (deathCount > 0)
                    {
                        Console.WriteLine($" [WARN ] Message '{message}' has been retried {deathCount} time(s).");
                    }

                    await Task.Delay(1000); // Simulate processing time

                    MessageOutcome outcome = RetryPolicy.Decide(
                        message.Contains("poison", StringComparison.OrdinalIgnoreCase),
                        deathCount,
                        maxRetries);

                    switch (outcome)
                    {
                        case MessageOutcome.Ack:
                            await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
                            Console.WriteLine($" [INFO ] Processing of message '{message}' succeeded.");

                            break;
                        case MessageOutcome.Retry:
                            Console.WriteLine($" [WARN ] Processing of message '{message}' failed; will retry.");
                            await channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);

                            break;
                        case MessageOutcome.Park:
                            Console.WriteLine($" [ERROR] Processing of message '{message}' failed; max retries reached.");

                            BasicProperties properties = new() { Persistent = true, Headers = ea.BasicProperties.Headers };
                            await channel.BasicPublishAsync(
                                exchange: ExchangeName.DeadLetter,
                                routingKey: RoutingKeyName.Dead,
                                mandatory: false,
                                basicProperties: properties,
                                body: body);

                            await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);

                            break;
                        default:
                            throw new InvalidOperationException($"Unhandled message outcome: {outcome}");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($" [ERROR] Exception during message processing: {ex.GetType().Name}: {ex.Message}. " +
                        "The message was not acknowledged.");

                    throw;
                }
                finally
                {
                    _channelSemaphore.Release();
                }
            };

            string consumerTag = await channel.BasicConsumeAsync(
                queue: QueueName.Work,
                autoAck: false,
                consumer: consumer,
                cancellationToken: stoppingToken);

            try
            {
                await Task.Delay(Timeout.Infinite, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                Console.WriteLine(" [WARN ] Cancellation requested; stopping service.");
            }

            try
            {
                using CancellationTokenSource cts = new(TimeSpan.FromSeconds(3));
                CancellationToken cleanupToken = cts.Token;

                await channel.BasicCancelAsync(consumerTag, cancellationToken: cleanupToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine($" [ERROR] Canceling the consumer failed: {ex.GetType().Name}: {ex.Message}");
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
                    Console.WriteLine($" [WARN ] Handler still running after {waitSeconds} seconds; its message will be redelivered.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($" [ERROR] Waiting for the gate failed: {ex.GetType().Name}: {ex.Message}");
            }

            try
            {
                using CancellationTokenSource cts = new(TimeSpan.FromSeconds(5));
                CancellationToken cleanupToken = cts.Token;

                await channel.CloseAsync(cleanupToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine($" [ERROR] Closing the channel failed: {ex.GetType().Name}: {ex.Message}");
            }

            try
            {
                using CancellationTokenSource cts = new(TimeSpan.FromSeconds(5));
                CancellationToken cleanupToken = cts.Token;

                await connection.CloseAsync(cleanupToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine($" [ERROR] Closing the connection failed: {ex.GetType().Name}: {ex.Message}");
            }
        }
    }
}
