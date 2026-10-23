using RabbitMqPractice.Messaging;
using RabbitMqPractice.Worker.Email;
using RabbitMqPractice.Worker.Practice;
using RabbitMqPractice.Worker.Store;

HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

// worker services
builder.Services.AddHostedService<EmailWorkerService>();
builder.Services.AddHostedService<PracticeWorkerService>();
builder.Services.AddHostedService<StoreWorkerService>();

builder.Services.AddSingleton<ConfigureConnectionFactory>();

IHost app = builder.Build();
app.Run();
