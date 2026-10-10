namespace Receiver.Tests;

public class RetryPolicyTest
{
    [Theory]
    [InlineData(false, 0, 2, MessageOutcome.Ack)]
    [InlineData(false, 5, 2, MessageOutcome.Ack)]
    [InlineData(true, 0, 2, MessageOutcome.Retry)]
    [InlineData(true, 1, 2, MessageOutcome.Retry)]
    [InlineData(true, 2, 2, MessageOutcome.Park)]
    [InlineData(true, 5, 2, MessageOutcome.Park)]
    [InlineData(true, 0, 0, MessageOutcome.Park)]
    public void Decide_ReturnsExpectedOutcome(bool processingFailed, long deathCount, long maxRetries, MessageOutcome expected)
    {
        MessageOutcome actual = RetryPolicy.Decide(processingFailed, deathCount, maxRetries);

        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(3, 4)]
    [InlineData(3L, 4L)]
    public void DeathCount_ReturnsCorrectCount(object count, object otherCount)
    {
        Dictionary<string, object?> headers = new()
        {
            ["x-death"] = new List<object>()
            {
                AddXDeathChildElement(count),
                AddXDeathChildElement(otherCount)
            }
        };
        
        Assert.Equal(3L, RetryPolicy.GetDeathCount(headers));
    }

    [Theory]
    [InlineData("abc")]
    [InlineData(null)]
    public void DeathCount_UnparseableCount_ThrowsInvalidOperationException(object? count)
    {
        Dictionary<string, object?> headers = new()
        {
            ["x-death"] = new List<object>()
            {
                AddXDeathChildElement(count)
            }
        };

        AssertThrowsInvalidOperationException(() => RetryPolicy.GetDeathCount(headers),
            "Failed to parse death count from 'x-death' header.");
    }

    [Fact]
    public void DeathCount_MissingCountKey_ThrowsInvalidOperationException()
    {
        Dictionary<string, object?> headers = new()
        {
            ["x-death"] = new List<object>()
            {
                new Dictionary<string, object>()
                {
                    // NOTE: Other than the key structure, only assign what's needed
                    ["reason"] = new byte[7],
                    ["queue"] = new byte[13],
                    ["time"] = new RabbitMQ.Client.AmqpTimestamp(),
                    ["exchange"] = new byte[14],
                    ["routing-keys"] = new List<object> { new byte[4] }
                }
            }
        };

        AssertThrowsInvalidOperationException(() => RetryPolicy.GetDeathCount(headers), 
            "Failed to parse death count from 'x-death' header.");
    }

    [Fact]
    public void DeathCount_WrongFirstEntryType_ThrowsInvalidOperationException()
    {
        Dictionary<string, object?> headers = new()
        {
            ["x-death"] = new List<object>()
            {
                "count"
            }
        };

        AssertThrowsInvalidOperationException(() => RetryPolicy.GetDeathCount(headers),
            "Failed to parse death count from 'x-death' header.");
    }

    [Fact]
    public void Empty_XDeath_Header_ThrowsInvalidOperationException()
    {
        Dictionary<string, object?> headers = new()
        {
            ["x-death"] = new List<object>()
        };

        AssertThrowsInvalidOperationException(() => RetryPolicy.GetDeathCount(headers),
            "'x-death' header structure has changed.");
    }

    [Fact]
    public void WrongType_XDeath_Header_ThrowsInvalidOperationException()
    {
        Dictionary<string, object?> headers = new()
        {
            ["x-death"] = new object[] { 1 }
        };

        AssertThrowsInvalidOperationException(() => RetryPolicy.GetDeathCount(headers),
            "'x-death' header structure has changed.");
    }

    [Fact]
    public void HeadersWithoutXDeath_ReturnsZero()
    {
        Assert.Equal(0L, RetryPolicy.GetDeathCount(new Dictionary<string, object?>()));
    }

    [Fact]
    public void NullHeader_ReturnsZero()
    {
        Assert.Equal(0L, RetryPolicy.GetDeathCount(null));
    }

    #region Private Helper Methods
    private static Dictionary<string, object?> AddXDeathChildElement(object? count)
    {
        return new Dictionary<string, object?>()
        {
            // NOTE: Other than the key structure, only assign what's needed
            ["count"] = count,
            ["reason"] = new byte[7],
            ["queue"] = new byte[13],
            ["time"] = new RabbitMQ.Client.AmqpTimestamp(),
            ["exchange"] = new byte[14],
            ["routing-keys"] = new List<object> { new byte[4] }
        };
    }

    private static void AssertThrowsInvalidOperationException(Action action, string expectedMessage)
    {
        var ex = Assert.Throws<InvalidOperationException>(action);
        Assert.Equal(expectedMessage, ex.Message);
    }
    #endregion
}
