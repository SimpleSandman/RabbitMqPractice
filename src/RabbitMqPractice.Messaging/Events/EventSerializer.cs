using System.Text.Json;

namespace RabbitMqPractice.Messaging.Events
{
    public static class EventSerializer
    {
        public const string ContentType = "application/json";

        private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

        public static byte[] Serialize<T>(T message) =>
            JsonSerializer.SerializeToUtf8Bytes(message, Options);

        public static T Deserialize<T>(ReadOnlySpan<byte> body) =>
            JsonSerializer.Deserialize<T>(body, Options)
                ?? throw new JsonException("The message body was null.");
    }
}