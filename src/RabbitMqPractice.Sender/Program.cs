using RabbitMQ.Client;
using RabbitMqPractice.Messaging;
using RabbitMqPractice.Messaging.Names;
using System.Text;

ConnectionFactory factory = new() { HostName = "localhost", UserName = "admin", Password = "password" };

using IConnection connection = await factory.CreateConnectionAsync();
using IChannel channel = await connection.CreateChannelAsync();

await Topology.DeclareWorkQueueAsync(channel);

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
