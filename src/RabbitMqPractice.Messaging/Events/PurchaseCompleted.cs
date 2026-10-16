namespace RabbitMqPractice.Messaging.Events
{
    public record PurchaseCompleted
    {
        public required Guid MessageId { get; init; }
        public required DateTimeOffset Timestamp { get; init; }
        public required int Quantity { get; init; }
        public required string ProductName { get; init; }
    }
}
