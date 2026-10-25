using RabbitMQ.Client;
using RabbitMqPractice.Api.Publishers.Interfaces;
using RabbitMqPractice.Messaging.Events;
using RabbitMqPractice.Messaging.Names;
using RabbitMqPractice.Messaging.Topology;

namespace RabbitMqPractice.Api.Publishers
{
    public sealed class RabbitMqPurchaseEventPublisher : IPurchaseEventPublisher, IAsyncDisposable
    {
        private readonly IConnection _connection;
        private readonly IChannel _channel;
        private readonly SemaphoreSlim _channelSemaphore = new(1, 1);

        private RabbitMqPurchaseEventPublisher(IConnection connection, IChannel channel)
        {
            _connection = connection;
            _channel = channel;
        }

        public static async Task<RabbitMqPurchaseEventPublisher> CreateAsync(ConnectionFactory factory, CancellationToken cancellationToken) 
        {
            IConnection connection = await factory.CreateConnectionAsync(cancellationToken: cancellationToken);

            try
            {
                IChannel channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

                await StoreTopology.DeclareExchangeAsync(channel, cancellationToken);

                return new RabbitMqPurchaseEventPublisher(connection, channel);
            }
            catch
            {
                await connection.DisposeAsync();
                throw;
            }
        }

        public async Task PublishAsync(PurchaseCompleted purchase, CancellationToken cancellationToken)
        {
            // Semaphore is used to ensure that only one thread can publish at a time,
            // preventing potential issues with concurrent access to the channel.
            await _channelSemaphore.WaitAsync(cancellationToken);

            try
            {
                byte[] serializedPurchase = EventSerializer.Serialize(purchase);

                BasicProperties properties = new() { ContentType = EventSerializer.ContentType, Persistent = true };

                await _channel.BasicPublishAsync(
                    exchange: ExchangeName.StoreEvents,
                    routingKey: RoutingKeyName.PurchaseCompleted,
                    mandatory: false,
                    basicProperties: properties,
                    body: serializedPurchase,
                    cancellationToken: CancellationToken.None); // Do not cancel the publish. Finish before the semaphore is released
            }
            finally
            {
                _channelSemaphore.Release();
            }
        }

        public async ValueTask DisposeAsync() 
        {
            try
            {
                using CancellationTokenSource cts = new(TimeSpan.FromSeconds(10));
                CancellationToken cleanupToken = cts.Token;

                const int waitSeconds = 5;
                if (await _channelSemaphore.WaitAsync(TimeSpan.FromSeconds(waitSeconds), cleanupToken))
                {
                    _channelSemaphore.Release();
                }
                else
                {
                    // TODO: Consider ILogger for logging instead of Console.WriteLine, but for simplicity, we use Console.WriteLine here.
                    Console.WriteLine($" [WARN ] Publisher still running after {waitSeconds} seconds.");
                }
            }
            catch (Exception ex)
            {
                // TODO: Consider ILogger for logging instead of Console.WriteLine, but for simplicity, we use Console.WriteLine here.
                Console.WriteLine($" [ERROR] Waiting for in-flight publishes failed: {ex.GetType().Name}: {ex.Message}");
            }

            try
            {
                using CancellationTokenSource cts = new(TimeSpan.FromSeconds(5));
                CancellationToken cleanupToken = cts.Token;

                await _channel.CloseAsync(cleanupToken);
            }
            catch (Exception ex)
            {
                // TODO: Consider ILogger for logging instead of Console.WriteLine, but for simplicity, we use Console.WriteLine here.
                Console.WriteLine($" [ERROR] Closing the channel failed: {ex.GetType().Name}: {ex.Message}");
            }

            try
            {
                using CancellationTokenSource cts = new(TimeSpan.FromSeconds(5));
                CancellationToken cleanupToken = cts.Token;

                await _connection.CloseAsync(cleanupToken);
            }
            catch (Exception ex)
            {
                // TODO: Consider ILogger for logging instead of Console.WriteLine, but for simplicity, we use Console.WriteLine here.
                Console.WriteLine($" [ERROR] Closing the connection failed: {ex.GetType().Name}: {ex.Message}");
            }

            await _channel.DisposeAsync();
            await _connection.DisposeAsync();
            _channelSemaphore.Dispose();
        }
    }
}
