using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMqPractice.Messaging;
using RabbitMqPractice.Messaging.Names;
using RabbitMqPractice.Messaging.Topology;
using System.Text;

HostApplicationBuilder builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
{
    ContentRootPath = AppContext.BaseDirectory
});
IConfiguration configuration = builder.Configuration;

ConfigureConnectionFactory connectionFactory = new(configuration);
ConnectionFactory factory = connectionFactory.CreateConnectionFactory();

await using IConnection connection = await factory.CreateConnectionAsync();
await using IChannel channel = await connection.CreateChannelAsync();

await PracticeTopology.DeclareWorkQueueAsync(channel);

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
