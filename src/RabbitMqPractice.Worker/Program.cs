using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMqPractice.Messaging.Names;
using RabbitMqPractice.Worker;
using System.Text;

ConnectionFactory factory = new() { HostName = "localhost", UserName = "admin", Password = "password" };

using IConnection connection = await factory.CreateConnectionAsync();
using IChannel channel = await connection.CreateChannelAsync();

#region Exchange and Queue Setup

await channel.ExchangeDeclareAsync(
    exchange: ExchangeName.DeadLetter,
    type: ExchangeType.Direct,
    durable: true,
    autoDelete: false);

await channel.QueueDeclareAsync(
    queue: QueueName.Dead,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: new Dictionary<string, object?>
    {
        { "x-queue-type", "classic" }
    });

await channel.QueueBindAsync(
    queue: QueueName.Dead,
    exchange: ExchangeName.DeadLetter,
    routingKey: RoutingKeyName.Dead);

await channel.ExchangeDeclareAsync(
    exchange: ExchangeName.Retry,
    type: ExchangeType.Direct,
    durable: true,
    autoDelete: false);

await channel.QueueDeclareAsync(
    queue: QueueName.Wait,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: new Dictionary<string, object?>
    {
        { "x-queue-type", "classic" },
        { "x-message-ttl", 5000 }, // 5 seconds
        { "x-dead-letter-exchange", ExchangeName.Default },
        { "x-dead-letter-routing-key", QueueName.Work }
    });

await channel.QueueBindAsync(
    queue: QueueName.Wait,
    exchange: ExchangeName.Retry,
    routingKey: RoutingKeyName.Wait);

await channel.QueueDeclareAsync(
    queue: QueueName.Work, 
    durable: true, 
    exclusive: false, 
    autoDelete: false,
    arguments: new Dictionary<string, object?> 
    {
        { "x-queue-type", "classic" },
        { "x-dead-letter-exchange", ExchangeName.Retry },
        { "x-dead-letter-routing-key", RoutingKeyName.Wait }
    });

await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

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

await channel.BasicConsumeAsync(queue: QueueName.Work, autoAck: false, consumer: consumer);

Console.WriteLine(" Press [enter] to exit program.");
Console.ReadLine();
