namespace RabbitMqPractice.Messaging.Names
{
    public static class RoutingKeyName
    {
        public const string Dead = "dead";
        public const string Wait = "wait";
        public const string PurchaseCompleted = "purchase.completed";
        public const string PurchaseDead = "purchase.dead";
        public const string PurchaseWait = "purchase.wait";
    }
}
