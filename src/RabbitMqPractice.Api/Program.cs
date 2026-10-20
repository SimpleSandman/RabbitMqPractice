using RabbitMQ.Client;
using RabbitMqPractice.Api.Models.Requests;
using RabbitMqPractice.Api.Publishers;
using RabbitMqPractice.Api.Publishers.Interfaces;
using RabbitMqPractice.Messaging;
using RabbitMqPractice.Messaging.Events;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddOpenApi();
builder.Services.AddValidation();

ConnectionFactory factory = new ConfigureConnectionFactory(builder.Configuration).CreateConnectionFactory();
factory.ClientProvidedName = "api";

RabbitMqPurchaseEventPublisher purchasePublisher = await RabbitMqPurchaseEventPublisher.CreateAsync(factory, CancellationToken.None);
builder.Services.AddSingleton<IPurchaseEventPublisher>(_ => purchasePublisher);

WebApplication app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.MapPost("/purchases", async (PurchaseRequest request, IPurchaseEventPublisher publisher, CancellationToken cancellationToken) =>
{
    PurchaseCompleted purchaseEvent = new()
    {
        MessageId = Guid.NewGuid(),
        Timestamp = DateTimeOffset.UtcNow,
        Quantity = request.Quantity,
        ProductName = request.ProductName
    };

    await publisher.PublishAsync(purchaseEvent, cancellationToken);

    return Results.Accepted(value: purchaseEvent);
});

app.Run();
