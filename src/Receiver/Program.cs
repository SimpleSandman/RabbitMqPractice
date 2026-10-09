using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Receiver;
using System.Text;

ConnectionFactory factory = new() { HostName = "localhost", UserName = "admin", Password = "password" };

using IConnection connection = await factory.CreateConnectionAsync();
using IChannel channel = await connection.CreateChannelAsync();

#region Exchange and Queue Setup

await channel.ExchangeDeclareAsync(
    exchange: "practice.dlx",
    type: ExchangeType.Direct,
    durable: true,
    autoDelete: false);

await channel.QueueDeclareAsync(
    queue: "practice-dead",
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: new Dictionary<string, object?>
    {
        { "x-queue-type", "classic" }
    });

await channel.QueueBindAsync(
    queue: "practice-dead",
    exchange: "practice.dlx",
    routingKey: "dead");

await channel.ExchangeDeclareAsync(
    exchange: "practice.retry",
    type: ExchangeType.Direct,
    durable: true,
    autoDelete: false);

await channel.QueueDeclareAsync(
    queue: "practice-wait",
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: new Dictionary<string, object?>
    {
        { "x-queue-type", "classic" },
        { "x-message-ttl", 5000 }, // 5 seconds
        { "x-dead-letter-exchange", string.Empty }, // Default exchange
        { "x-dead-letter-routing-key", "practice-queue" }
    });

await channel.QueueBindAsync(
    queue: "practice-wait",
    exchange: "practice.retry",
    routingKey: "wait");

await channel.QueueDeclareAsync(
    queue: "practice-queue", 
    durable: true, 
    exclusive: false, 
    autoDelete: false,
    arguments: new Dictionary<string, object?> 
    {
        { "x-queue-type", "classic" },
        { "x-dead-letter-exchange", "practice.retry" },
        { "x-dead-letter-routing-key", "wait" }
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
                exchange: "practice.dlx",
                routingKey: "dead",
                mandatory: false,
                basicProperties: properties,
                body: body);

            await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);

            break;
        default:
            throw new InvalidOperationException("Unhandled message outcome");
    }
};

await channel.BasicConsumeAsync(queue: "practice-queue", autoAck: false, consumer: consumer);

Console.WriteLine(" Press [enter] to exit program.");
Console.ReadLine();
