using RabbitMqPractice.Messaging;
using RabbitMqPractice.Worker;

HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<WorkerService>();

builder.Services.AddSingleton<ConfigureConnectionFactory>();

IHost app = builder.Build();
app.Run();
