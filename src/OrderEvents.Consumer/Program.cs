using Microsoft.EntityFrameworkCore;
using OrderEvents.Consumer.AI;
using OrderEvents.Consumer.Configuration;
using OrderEvents.Consumer.Consumers;
using OrderEvents.Consumer.Persistence;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<KafkaConsumerSettings>(
    builder.Configuration.GetSection(KafkaConsumerSettings.SectionName));

builder.Services.Configure<ClaudeAiSettings>(
    builder.Configuration.GetSection(ClaudeAiSettings.SectionName));
builder.Services.AddHttpClient<IFailureAnalyzer, ClaudeFailureAnalyzer>();

// AddDbContextFactory (not AddDbContext) is the key registration: it hands out
// a factory instead of a single scoped instance, so the BackgroundService below
// can create one DbContext per message rather than sharing one across concurrent
// operations.
builder.Services.AddDbContextFactory<OrdersDbContext>(options =>
    options.UseSqlServer(builder.Configuration.GetConnectionString("DefaultConnection")));

builder.Services.AddSingleton<OrderRecordProjector>();
builder.Services.AddHostedService<OrderPlacedConsumer>();

var host = builder.Build();
host.Run();
