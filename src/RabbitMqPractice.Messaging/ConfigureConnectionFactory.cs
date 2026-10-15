using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;

namespace RabbitMqPractice.Messaging
{
    public class ConfigureConnectionFactory(IConfiguration configuration)
    {
        public ConnectionFactory CreateConnectionFactory()
        {
            return new()
            {
                HostName = configuration["RabbitMQ:HostName"] ?? throw new InvalidOperationException("HostName is not configured"),
                UserName = configuration["RabbitMQ:UserName"] ?? throw new InvalidOperationException("UserName is not configured"),
                Password = configuration["RabbitMQ:Password"] ?? throw new InvalidOperationException("Password is not configured")
            };
        }
    }
}
