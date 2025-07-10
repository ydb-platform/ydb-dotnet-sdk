using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Topic.Tests;

public static class Utils
{
    public static readonly ILoggerFactory LoggerFactory =
        Microsoft.Extensions.Logging.LoggerFactory.Create(builder => builder.AddConsole());
}
