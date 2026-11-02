using RabbitMQ.Client.Events;
using RabbitMqPractice.Messaging;
using RabbitMqPractice.Messaging.Events;
using RabbitMqPractice.Messaging.Names;
using RabbitMqPractice.Messaging.Topology;
using RabbitMqPractice.Worker.Common;

namespace RabbitMqPractice.Worker.Email
{
    public sealed class EmailWorkerService(ConfigureConnectionFactory connectionFactory, IConfiguration configuration) 
        : ConsumerServiceBase(connectionFactory, new EmailTopology(), configuration)
    {
        protected override string ServiceName => "email";
        protected override string ConsumeQueue => QueueName.PurchaseEmail;
        protected override string ParkRoutingKey => RoutingKeyName.PurchaseEmailDead;
        protected override string WaitingMessage => "Waiting for any purchase messages to email.";

        protected override async Task<ConsumerResult> HandleAsync(BasicDeliverEventArgs ea, CancellationToken stoppingToken)
        {
            try
            {
                byte[] body = ea.Body.ToArray();
                PurchaseCompleted purchaseCompleted = EventSerializer.Deserialize<PurchaseCompleted>(body);

                Log("DEBUG", $"Received PurchaseCompleted message '{purchaseCompleted.MessageId}'");
                return new ConsumerResult(MessageOutcome.Ack, purchaseCompleted.MessageId.ToString());
            }
            catch (Exception ex)
            {
                Log("ERROR", $"Exception while reading the message: {ex.GetType().Name}: {ex.Message}.");
                return new ConsumerResult(MessageOutcome.Park, null);
            }
        }
    }
}
