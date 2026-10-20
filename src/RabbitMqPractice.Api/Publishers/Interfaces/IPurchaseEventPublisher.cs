using RabbitMqPractice.Messaging.Events;

namespace RabbitMqPractice.Api.Publishers.Interfaces
{
    public interface IPurchaseEventPublisher
    {
        Task PublishAsync(PurchaseCompleted purchase, CancellationToken cancellationToken);
    }
}
