using RabbitMQ.Client;
using System.Text;

ConnectionFactory factory = new() { HostName = "localhost", UserName = "admin", Password = "password" };

using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

await channel.QueueDeclareAsync(
    queue: "practice-queue", 
    durable: true, 
    exclusive: false, 
    autoDelete: false,
    arguments: new Dictionary<string, object?> { { "x-queue-type", "classic" } });

const string message = "ticket 5";
byte[] body = Encoding.UTF8.GetBytes(message);

await channel.BasicPublishAsync(exchange: string.Empty, routingKey: "practice-queue", body: body);
Console.WriteLine($" [x] Sent {message}");

Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();
