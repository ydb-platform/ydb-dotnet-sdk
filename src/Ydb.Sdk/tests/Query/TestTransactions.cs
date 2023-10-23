using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Xunit.Abstractions;
using Ydb.Sdk.Services.Query;

namespace Ydb.Sdk.Tests.Query;

[Trait("Category", "Integration")]
public class TestTransactions
{
    private readonly ILoggerFactory _loggerFactory;

    private readonly DriverConfig _driverConfig = new(
        endpoint: "grpc://localhost:2136",
        database: "/local"
    );


    public TestTransactions()
    {
        _loggerFactory = Utils.GetLoggerFactory() ?? NullLoggerFactory.Instance;
        _loggerFactory.CreateLogger<TestExecuteQuery>();
    }
}