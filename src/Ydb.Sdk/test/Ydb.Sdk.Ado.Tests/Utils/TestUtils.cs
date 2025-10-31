using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Ado.Tests.Utils;

public static class TestUtils
{
    private const string DefaultConnectionString = "Host=localhost;Port=2136;Database=local;MaxPoolSize=10";

    public static string ConnectionString =>
        Environment.GetEnvironmentVariable("YDB_TEST_DB") ?? DefaultConnectionString;

    public static readonly ILoggerFactory LoggerFactory =
        Microsoft.Extensions.Logging.LoggerFactory.Create(builder => builder.AddConsole());
}
