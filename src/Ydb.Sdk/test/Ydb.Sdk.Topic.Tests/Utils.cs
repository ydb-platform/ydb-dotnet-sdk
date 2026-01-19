using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Topic.Tests;

public static class Utils
{
    public static readonly ILoggerFactory LoggerFactory =
        Microsoft.Extensions.Logging.LoggerFactory.Create(builder => builder.AddConsole());

    public const string ConnectionString = "Host=localhost;Port=2136;Database=/local";
}
