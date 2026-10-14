using RabbitMQ.Client;
using RabbitMqPractice.Messaging.Names;
using System.Text;

ConnectionFactory factory = new() { HostName = "localhost", UserName = "admin", Password = "password" };

using IConnection connection = await factory.CreateConnectionAsync();
using IChannel channel = await connection.CreateChannelAsync();

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

string message = args.Length > 0 ? string.Join(" ", args) : "ticket A";
byte[] body = Encoding.UTF8.GetBytes(message);

BasicProperties properties = new() { Persistent = true };

await channel.BasicPublishAsync(
    exchange: ExchangeName.Default, 
    routingKey: QueueName.Work, 
    mandatory: false, 
    basicProperties: properties,
    body: body);

Console.WriteLine($" [x] Sent {message}");
