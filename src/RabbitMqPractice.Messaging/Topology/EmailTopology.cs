using RabbitMQ.Client;
using RabbitMqPractice.Messaging.Names;

namespace RabbitMqPractice.Messaging.Topology
{
    public class EmailTopology : TopologyBase
    {
        public override async Task DeclareAsync(IChannel channel, CancellationToken cancellationToken = default)
        {
            await DeclareStoreEventsExchangeAsync(channel, cancellationToken);

            await channel.QueueDeclareAsync(
                queue: QueueName.PurchaseEmail,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: new Dictionary<string, object?>
                {
                    { "x-queue-type", "classic" }
                },
                cancellationToken: cancellationToken);

            await channel.QueueBindAsync(
                queue: QueueName.PurchaseEmail,
                exchange: ExchangeName.StoreEvents,
                routingKey: RoutingKeyName.PurchaseWildcard,
                cancellationToken: cancellationToken);

            await DeclareDeadLetterQueueAsync(channel, QueueName.PurchaseEmailDead, RoutingKeyName.PurchaseEmailDead, cancellationToken);
        }
    }
}
