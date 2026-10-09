using RabbitMQ.Client;
using RabbitMQ.Client.Events;
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

Console.WriteLine(" [*] Waiting for messages.");

AsyncEventingBasicConsumer consumer = new(channel);
consumer.ReceivedAsync += async (model, ea) =>
{
    byte[] body = ea.Body.ToArray();
    string message = Encoding.UTF8.GetString(body);
    Console.WriteLine($" [x] Received {message}");

    long numRetries = 0L;
    IDictionary<string, object?>? headers = ea.BasicProperties.Headers;

    if (headers is not null
        && headers.TryGetValue("x-death", out object? deathObj)
        && deathObj is List<object> deathHeader
        && deathHeader.Count > 0)
    {
        if (deathHeader[0] is Dictionary<string, object> latestEntry
            && latestEntry.TryGetValue("count", out object? countObj)
            && long.TryParse(countObj?.ToString(), out numRetries))
        {
            Console.WriteLine($" [x] Death count: {numRetries}");
        }
        else
        {
            throw new InvalidOperationException("Failed to parse death count from 'x-death' header.");
        }
    }

    await Task.Delay(1000); // Simulate processing time

    if (message.Contains("poison", StringComparison.OrdinalIgnoreCase))
    {
        if (numRetries >= maxRetries)
        {
            Console.WriteLine(" [!] Max retry attempts reached. Moving to dead letter queue.");

            BasicProperties properties = new() { Persistent = true, Headers = ea.BasicProperties.Headers };
            await channel.BasicPublishAsync(
                exchange: "practice.dlx", 
                routingKey: "dead", 
                mandatory: false, 
                basicProperties: properties, 
                body: body);

            await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
        }
        else
        {
            Console.WriteLine(" [!] Poison message detected. Rejecting without requeue.");
            await channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
        }
    }
    else
    {
        Console.WriteLine(" [o] Done processing message.");
        await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
    }
};

await channel.BasicConsumeAsync(queue: "practice-queue", autoAck: false, consumer: consumer);

Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();
