using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Coordination.Tests;

public static class Utils
{
    public const string ConnectionString = "Host=localhost;Port=2136;Database=/local";

    public static readonly ILoggerFactory LoggerFactory =
        Microsoft.Extensions.Logging.LoggerFactory.Create(builder => builder.AddConsole());
}
