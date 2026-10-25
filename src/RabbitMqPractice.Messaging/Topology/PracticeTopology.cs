using RabbitMQ.Client;
using RabbitMqPractice.Messaging.Names;

namespace RabbitMqPractice.Messaging.Topology
{
    public class PracticeTopology : TopologyBase
    {
        public static async Task DeclareWorkQueueAsync(IChannel channel, CancellationToken cancellationToken = default)
        {
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
                },
                cancellationToken: cancellationToken);
        }

        public override async Task DeclareAsync(IChannel channel, CancellationToken cancellationToken = default)
        {
            await DeclareDeadLetterQueueAsync(channel, QueueName.Dead, RoutingKeyName.Dead, cancellationToken);

            await DeclareRetryLoopAsync(channel, ExchangeName.Retry, QueueName.Wait, RoutingKeyName.Wait, QueueName.Work, cancellationToken);

            await DeclareWorkQueueAsync(channel, cancellationToken);
        }
    }
}
