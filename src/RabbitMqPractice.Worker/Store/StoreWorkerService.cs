using RabbitMQ.Client.Events;
using RabbitMqPractice.Messaging;
using RabbitMqPractice.Messaging.Events;
using RabbitMqPractice.Messaging.Names;
using RabbitMqPractice.Messaging.Topology;
using RabbitMqPractice.Worker.Common;

namespace RabbitMqPractice.Worker.Store
{
    public sealed class StoreWorkerService(ConfigureConnectionFactory connectionFactory, IConfiguration configuration) 
        : ConsumerServiceBase(connectionFactory, new StoreTopology(), configuration)
    {
        protected override string ServiceName => "store";
        protected override string ConsumeQueue => QueueName.PurchaseProcessing;
        protected override string ParkRoutingKey => RoutingKeyName.PurchaseDead;
        protected override string WaitingMessage => "Waiting for any purchase messages to store.";
        protected override bool SupportsRetry => true;
        
        protected override async Task<ConsumerResult> HandleAsync(BasicDeliverEventArgs ea, CancellationToken stoppingToken) 
        {
            try
            {
                byte[] body = ea.Body.ToArray();
                PurchaseCompleted purchaseCompleted = EventSerializer.Deserialize<PurchaseCompleted>(body);
                
                Log("DEBUG", $"Received PurchaseCompleted message '{purchaseCompleted.MessageId}'");

                long deathCount = RetryPolicy.GetDeathCount(ea.BasicProperties.Headers);
                if (deathCount > 0)
                {
                    Log("WARN", $"Message '{purchaseCompleted.MessageId}' has been retried {deathCount} time(s).");
                }

                MessageOutcome outcome = RetryPolicy.Decide(
                    purchaseCompleted.ProductName.Contains("poison", StringComparison.OrdinalIgnoreCase),
                    deathCount,
                    2L);

                return new ConsumerResult(outcome, purchaseCompleted.MessageId.ToString());
            }
            catch (Exception ex)
            {
                Log("ERROR", $"Exception while reading the message: {ex.GetType().Name}: {ex.Message}.");
                return new ConsumerResult(MessageOutcome.Park, null);
            }
        }
    }
}
