using RabbitMqPractice.Messaging.Events;
using System.Text;
using System.Text.Json;

namespace RabbitMqPractice.Messaging.Tests;

public class PurchaseTest
{
    [Fact]
    public void EventSerialization_Deserialization_ShouldPreserveData()
    {
        PurchaseCompleted purchase = new()
        {
            MessageId = Guid.NewGuid(),
            Timestamp = DateTimeOffset.UtcNow,
            Quantity = 1,
            ProductName = "Test Product"
        };

        byte[] serializedPurchase = EventSerializer.Serialize(purchase);

        PurchaseCompleted deserializedPurchase = EventSerializer.Deserialize<PurchaseCompleted>(serializedPurchase);

        Assert.Equal(purchase, deserializedPurchase);
    }

    [Theory]
    [InlineData("messageId")]
    [InlineData("timestamp")]
    [InlineData("quantity")]
    [InlineData("productName")]
    public void EnsureCamelCaseSerialization(string propertyName)
    {
        PurchaseCompleted purchase = new()
        {
            MessageId = Guid.NewGuid(),
            Timestamp = DateTimeOffset.UtcNow,
            Quantity = 1,
            ProductName = "Test Product"
        };

        byte[] body = EventSerializer.Serialize(purchase);

        using JsonDocument document = JsonDocument.Parse(body);

        Assert.True(document.RootElement.TryGetProperty(propertyName, out _));
    }

    [Fact]
    public void MissingRequiredElements_ThrowsJsonException()
    {
        string jsonString =
            """
            { "messageId": "a285d9fd-72fc-4652-ba00-fca666ef7cef", "timestamp": "2026-10-19T04:00:32.4315674+00:00" }
            """;

        byte[] body = Encoding.UTF8.GetBytes(jsonString);

        JsonException exception = Assert.Throws<JsonException>(
            () => EventSerializer.Deserialize<PurchaseCompleted>(body));

        Assert.Contains("required", exception.Message);
    }

    [Fact]
    public void MalformedJson_ThrowsJsonException()
    {
        string jsonString =
            """
            { "messageId": "a285d9fd-72fc-4652-ba00-fca666ef7cef", "quantity": 1 
            """;
        
        byte[] body = Encoding.UTF8.GetBytes(jsonString);

        Assert.Throws<JsonException>(() => EventSerializer.Deserialize<PurchaseCompleted>(body));
    }

    [Fact]
    public void Deserialize_NullLiteral_ThrowsJsonException()
    {
        byte[] body = Encoding.UTF8.GetBytes("null");

        JsonException exception = Assert.Throws<JsonException>(
            () => EventSerializer.Deserialize<PurchaseCompleted>(body));

        Assert.Contains("null", exception.Message);
    }

    [Theory]
    [InlineData("""{ "messageId": "a285d9fd-72fc-4652-ba00-fca666ef7cef", "timestamp": "2026-10-19T04:00:32.4315674+00:00", "quantity": 1, "productName": "x" }""", TestDisplayName = "CamelCase names")]
    [InlineData("""{ "MessageId": "a285d9fd-72fc-4652-ba00-fca666ef7cef", "Timestamp": "2026-10-19T04:00:32.4315674+00:00", "Quantity": 1, "ProductName": "x" }""", TestDisplayName = "PascalCase names")]
    [InlineData("""{ "messageId": "a285d9fd-72fc-4652-ba00-fca666ef7cef", "timestamp": "2026-10-19T04:00:32.4315674+00:00", "quantity": 1, "productName": "x", "extra": true }""", TestDisplayName = "Unknown extra property")]
    public void Deserialize_ValidBodies_Succeeds(string json)
    {
        PurchaseCompleted result = EventSerializer.Deserialize<PurchaseCompleted>(Encoding.UTF8.GetBytes(json));

        Assert.Equal(1, result.Quantity);
    }

    [Theory]
    [InlineData("", TestDisplayName = "An empty body")]
    [InlineData("""{ "messageId": "a285d9fd-72fc-4652-ba00-fca666ef7cef", "quantity": 1""", TestDisplayName = "Truncated JSON")]
    [InlineData("null", TestDisplayName = "The null literal")]
    [InlineData("[]", TestDisplayName = "An array instead of an object")]
    [InlineData("{}", TestDisplayName = "An empty object")]
    [InlineData("""{ "messageId": "a285d9fd-72fc-4652-ba00-fca666ef7cef", "timestamp": "2026-10-19T04:00:32.4315674+00:00", "quantity": "abc", "productName": "x" }""", TestDisplayName = "A quantity that is not a number")]
    [InlineData("""{ "messageId": "a285d9fd-72fc-4652-ba00-fca666ef7cef", "timestamp": "2026-10-19T04:00:32.4315674+00:00", "productName": "x" }""", TestDisplayName = "A missing quantity")]
    public void Deserialize_InvalidBodies_ThrowsJsonException(string json)
    {
        byte[] body = Encoding.UTF8.GetBytes(json);

        Assert.Throws<JsonException>(() => EventSerializer.Deserialize<PurchaseCompleted>(body));
    }
}
