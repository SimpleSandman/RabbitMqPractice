using RabbitMqPractice.Worker;

HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<WorkerService>();

IHost app = builder.Build();
app.Run();
