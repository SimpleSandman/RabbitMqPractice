using RabbitMqPractice.Messaging;
using RabbitMqPractice.Worker;

HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

// worker services
builder.Services.AddHostedService<PracticeWorkerService>();
builder.Services.AddHostedService<StoreWorkerService>();

builder.Services.AddSingleton<ConfigureConnectionFactory>();

IHost app = builder.Build();
app.Run();
