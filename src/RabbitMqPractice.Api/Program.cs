using RabbitMqPractice.Api.Models.Requests;
using RabbitMqPractice.Messaging.Events;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddOpenApi();
builder.Services.AddValidation();

WebApplication app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.MapPost("/purchases", (PurchaseRequest request) =>
{
    PurchaseCompleted purchaseEvent = new()
    {
        MessageId = Guid.NewGuid(),
        Timestamp = DateTimeOffset.UtcNow,
        Quantity = request.Quantity,
        ProductName = request.ProductName
    };

    return Results.Ok(purchaseEvent);
});

app.Run();
