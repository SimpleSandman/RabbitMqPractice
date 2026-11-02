using RabbitMQ.Client.Events;
using RabbitMqPractice.Messaging;
using RabbitMqPractice.Messaging.Names;
using RabbitMqPractice.Messaging.Topology;
using RabbitMqPractice.Worker.Common;
using System.Text;

namespace RabbitMqPractice.Worker.Practice
{
    public sealed class PracticeWorkerService(ConfigureConnectionFactory connectionFactory, IConfiguration configuration) 
        : ConsumerServiceBase(connectionFactory, new PracticeTopology(), configuration)
    {
        protected override string ServiceName => "practice";
        protected override string ConsumeQueue => QueueName.Work;
        protected override string ParkRoutingKey => RoutingKeyName.Dead;
        protected override string WaitingMessage => "Waiting for any practice messages.";
        protected override bool SupportsRetry => true;

        protected override async Task<ConsumerResult> HandleAsync(BasicDeliverEventArgs ea, CancellationToken stoppingToken)
        {
            byte[] body = ea.Body.ToArray();
            string message = Encoding.UTF8.GetString(body);
            Log("DEBUG", $"Received '{message}'");

            long deathCount = RetryPolicy.GetDeathCount(ea.BasicProperties.Headers);
            if (deathCount > 0)
            {
                Log("WARN", $"Message '{message}' has been retried {deathCount} time(s).");
            }

            MessageOutcome outcome = RetryPolicy.Decide(
                message.Contains("poison", StringComparison.OrdinalIgnoreCase),
                deathCount,
                2L);

            return new ConsumerResult(outcome, message);
        }
    }
}
