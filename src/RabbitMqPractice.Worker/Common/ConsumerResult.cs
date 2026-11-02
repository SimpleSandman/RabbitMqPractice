namespace RabbitMqPractice.Worker.Common
{
    public readonly record struct ConsumerResult(MessageOutcome Outcome, string? MessageLabel);
}
