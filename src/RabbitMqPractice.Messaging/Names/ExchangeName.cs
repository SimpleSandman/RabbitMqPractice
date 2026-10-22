namespace RabbitMqPractice.Messaging.Names
{
    public static class ExchangeName
    {
        public const string Default = "";
        public const string DeadLetter = "practice.dlx";
        public const string Retry = "practice.retry";
        public const string StoreEvents = "store.events";
        public const string PurchaseRetry = "purchase.retry";
    }
}
