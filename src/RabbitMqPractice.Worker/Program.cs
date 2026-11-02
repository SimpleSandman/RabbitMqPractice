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

// Each service owns its own connection and channel, so nothing is shared, and a slow drain should not delay the others.
builder.Services.Configure<HostOptions>(options => options.ServicesStopConcurrently = true);

IHost app = builder.Build();
app.Run();
