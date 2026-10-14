using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMqPractice.Messaging;
using RabbitMqPractice.Messaging.Names;
using System.Text;

namespace RabbitMqPractice.Worker
{
    public sealed class WorkerService : BackgroundService
    {
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            ConnectionFactory factory = new() { HostName = "localhost", UserName = "admin", Password = "password" };

            using IConnection connection = await factory.CreateConnectionAsync(cancellationToken: stoppingToken);
            using IChannel channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);

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
                        Console.WriteLine($" [INFO ] Processing of message '{message}' succeeded.");
                        await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);

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
            };

            await channel.BasicConsumeAsync(
                queue: QueueName.Work,
                autoAck: false,
                consumer: consumer,
                cancellationToken: stoppingToken);

            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
    }
}
