using RabbitMQ.Client;
using RabbitMqPractice.Messaging.Names;

namespace RabbitMqPractice.Messaging
{
    public static class Topology
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

        public static async Task DeclareRetryDeadLetterAsync(IChannel channel, CancellationToken cancellationToken = default)
        {
            // Setup Dead Letter Exchange and Queue
            await channel.ExchangeDeclareAsync(
                exchange: ExchangeName.DeadLetter,
                type: ExchangeType.Direct,
                durable: true,
                autoDelete: false,
                cancellationToken: cancellationToken);

            await channel.QueueDeclareAsync(
                queue: QueueName.Dead,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: new Dictionary<string, object?>
                {
                    { "x-queue-type", "classic" }
                },
                cancellationToken: cancellationToken);

            await channel.QueueBindAsync(
                queue: QueueName.Dead,
                exchange: ExchangeName.DeadLetter,
                routingKey: RoutingKeyName.Dead,
                cancellationToken: cancellationToken);

            // Setup Retry Exchange and Wait Queue
            await channel.ExchangeDeclareAsync(
                exchange: ExchangeName.Retry,
                type: ExchangeType.Direct,
                durable: true,
                autoDelete: false,
                cancellationToken: cancellationToken);

            await channel.QueueDeclareAsync(
                queue: QueueName.Wait,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: new Dictionary<string, object?>
                {
                    { "x-queue-type", "classic" },
                    { "x-message-ttl", 5000 }, // 5 seconds
                    { "x-dead-letter-exchange", ExchangeName.Default },
                    { "x-dead-letter-routing-key", QueueName.Work }
                },
                cancellationToken: cancellationToken);

            await channel.QueueBindAsync(
                queue: QueueName.Wait,
                exchange: ExchangeName.Retry,
                routingKey: RoutingKeyName.Wait,
                cancellationToken: cancellationToken);
        }

        public static async Task DeclareStoreEventsAsync(IChannel channel, CancellationToken cancellationToken = default)
        {
            await channel.ExchangeDeclareAsync(
                exchange: ExchangeName.StoreEvents,
                type: ExchangeType.Topic,
                durable: true,
                autoDelete: false,
                cancellationToken: cancellationToken);
        }

        public static async Task DeclareStoreQueueAsync(IChannel channel, CancellationToken cancellationToken = default)
        {
            await DeclareStoreEventsAsync(channel, cancellationToken);

            await channel.QueueDeclareAsync(
                queue: QueueName.PurchaseProcessing,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: new Dictionary<string, object?>
                {
                    { "x-queue-type", "classic" }
                },
                cancellationToken: cancellationToken);

            await channel.QueueBindAsync(
                queue: QueueName.PurchaseProcessing,
                exchange: ExchangeName.StoreEvents,
                routingKey: RoutingKeyName.PurchaseCompleted,
                cancellationToken: cancellationToken);
        }

        public static async Task DeclarePurchaseDlxQueueAsync(IChannel channel, CancellationToken cancellationToken = default)
        {
            await channel.ExchangeDeclareAsync(
                exchange: ExchangeName.DeadLetter,
                type: ExchangeType.Direct,
                durable: true,
                autoDelete: false,
                cancellationToken: cancellationToken);

            await channel.QueueDeclareAsync(
                queue: QueueName.PurchaseDead,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: new Dictionary<string, object?>
                {
                    { "x-queue-type", "classic" }
                },
                cancellationToken: cancellationToken);

            await channel.QueueBindAsync(
                queue: QueueName.PurchaseDead,
                exchange: ExchangeName.DeadLetter,
                routingKey: RoutingKeyName.PurchaseDead,
                cancellationToken: cancellationToken);
        }
    }
}
