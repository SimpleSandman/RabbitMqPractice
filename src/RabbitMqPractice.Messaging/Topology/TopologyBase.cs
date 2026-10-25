using RabbitMQ.Client;
using RabbitMqPractice.Messaging.Names;

namespace RabbitMqPractice.Messaging.Topology
{
    public abstract class TopologyBase
    {
        public abstract Task DeclareAsync(IChannel channel, CancellationToken cancellationToken = default);

        protected static async Task DeclareDeadLetterQueueAsync(
            IChannel channel, 
            string queue, 
            string routingKey, 
            CancellationToken cancellationToken = default) 
        {
            await channel.ExchangeDeclareAsync(
                exchange: ExchangeName.DeadLetter,
                type: ExchangeType.Direct,
                durable: true,
                autoDelete: false,
                cancellationToken: cancellationToken);

            await channel.QueueDeclareAsync(
                queue: queue,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: new Dictionary<string, object?>
                {
                    { "x-queue-type", "classic" }
                },
                cancellationToken: cancellationToken);

            await channel.QueueBindAsync(
                queue: queue,
                exchange: ExchangeName.DeadLetter,
                routingKey: routingKey,
                cancellationToken: cancellationToken);
        }

        protected static async Task DeclareRetryLoopAsync(
            IChannel channel, 
            string retryExchange, 
            string waitQueue, 
            string waitRoutingKey,
            string returnQueue,
            CancellationToken cancellationToken = default) 
        {
            await channel.ExchangeDeclareAsync(
                exchange: retryExchange,
                type: ExchangeType.Direct,
                durable: true,
                autoDelete: false,
                cancellationToken: cancellationToken);

            await channel.QueueDeclareAsync(
                queue: waitQueue,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: new Dictionary<string, object?>
                {
                    { "x-queue-type", "classic" },
                    { "x-message-ttl", 5000 }, // 5 seconds
                    { "x-dead-letter-exchange", ExchangeName.Default },
                    { "x-dead-letter-routing-key", returnQueue }
                },
                cancellationToken: cancellationToken);

            await channel.QueueBindAsync(
                queue: waitQueue,
                exchange: retryExchange,
                routingKey: waitRoutingKey,
                cancellationToken: cancellationToken);
        }

        protected static async Task DeclareStoreEventsExchangeAsync(IChannel channel, CancellationToken cancellationToken = default) 
        {
            await channel.ExchangeDeclareAsync(
                exchange: ExchangeName.StoreEvents,
                type: ExchangeType.Topic,
                durable: true,
                autoDelete: false,
                cancellationToken: cancellationToken);
        }
    }
}
