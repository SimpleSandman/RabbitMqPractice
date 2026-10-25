using RabbitMQ.Client;
using RabbitMqPractice.Messaging.Names;

namespace RabbitMqPractice.Messaging.Topology
{
    public class StoreTopology : TopologyBase
    {
        public static async Task DeclareExchangeAsync(IChannel channel, CancellationToken cancellationToken = default)
        {
            await DeclareStoreEventsExchangeAsync(channel, cancellationToken);
        }

        public override async Task DeclareAsync(IChannel channel, CancellationToken cancellationToken = default)
        {
            await DeclareStoreEventsExchangeAsync(channel, cancellationToken);

            await channel.QueueDeclareAsync(
                queue: QueueName.PurchaseProcessing,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: new Dictionary<string, object?>
                {
                    { "x-queue-type", "classic" },
                    { "x-dead-letter-exchange", ExchangeName.PurchaseRetry },
                    { "x-dead-letter-routing-key", RoutingKeyName.PurchaseWait }
                },
                cancellationToken: cancellationToken);

            await channel.QueueBindAsync(
                queue: QueueName.PurchaseProcessing,
                exchange: ExchangeName.StoreEvents,
                routingKey: RoutingKeyName.PurchaseCompleted,
                cancellationToken: cancellationToken);

            await DeclareDeadLetterQueueAsync(channel, QueueName.PurchaseDead, RoutingKeyName.PurchaseDead, cancellationToken);

            await DeclareRetryLoopAsync(channel, ExchangeName.PurchaseRetry, QueueName.PurchaseWait, RoutingKeyName.PurchaseWait, QueueName.PurchaseProcessing, cancellationToken);
        }
    }
}
