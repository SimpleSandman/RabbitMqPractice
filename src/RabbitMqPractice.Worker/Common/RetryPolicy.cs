namespace RabbitMqPractice.Worker.Common
{
    public enum MessageOutcome { Ack, Retry, Park }

    public static class RetryPolicy
    {
        public static long GetDeathCount(IDictionary<string, object?>? headers)
        {
            if (headers is not null && headers.TryGetValue("x-death", out object? deathObj))
            {
                if (deathObj is not List<object> deathHeader || deathHeader.Count == 0)
                {
                    throw new InvalidOperationException("'x-death' header structure has changed.");
                }

                if (deathHeader[0] is Dictionary<string, object> latestEntry
                    && latestEntry.TryGetValue("count", out object? countObj)
                    && long.TryParse(countObj?.ToString(), out long deathCount))
                {
                    return deathCount;
                }
                else
                {
                    throw new InvalidOperationException("Failed to parse death count from 'x-death' header.");
                }
            }

            return 0L;
        }

        public static MessageOutcome Decide(bool processingFailed, long deathCount, long maxRetries)
        {
            if (!processingFailed)
            {
                return MessageOutcome.Ack;
            }

            if (deathCount >= maxRetries)
            {
                return MessageOutcome.Park;
            }

            return MessageOutcome.Retry;
        }
    }
}
