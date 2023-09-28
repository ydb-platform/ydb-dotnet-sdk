using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Xunit.Abstractions;
using Ydb.Sdk.Auth;
using Ydb.Sdk.Services.Table;

namespace Ydb.Sdk.Tests.Auth;

[Trait("Category", "Integration")]
public class TestStaticAuth : IDisposable
{
    // ReSharper disable once NotAccessedField.Local
    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory? _loggerFactory;
    private readonly ILogger _logger;

    private readonly Driver _anonDriver;
    private readonly TableClient _anonTableClient;

    public TestStaticAuth(ITestOutputHelper output)
    {
        _output = output;
        _loggerFactory = Utils.GetLoggerFactory() ?? NullLoggerFactory.Instance;
        _logger = _loggerFactory.CreateLogger<TestStaticAuth>();

        var driverConfig = new DriverConfig(
            endpoint: "grpc://localhost:2136",
            database: "/local"
        );

        _anonDriver = new Driver(driverConfig, _loggerFactory);
        _anonDriver.Initialize().Wait();

        _anonTableClient = new TableClient(_anonDriver);
    }

    public void Dispose()
    {
        _anonTableClient.Dispose();
        _anonDriver.Dispose();
        GC.SuppressFinalize(this);
    }

    private async Task DoAuth(string? passwordCreate, string? passwordAuth, int maxRetries = 5)
    {
        _logger.LogInformation("Creating anon driver");
        var anonDriverConfig = new DriverConfig(
            endpoint: "grpc://localhost:2136",
            database: "/local"
        );

        await using var anonDriver = await Driver.CreateInitialized(anonDriverConfig, _loggerFactory);

        _logger.LogInformation("Anon driver created");
        using var anonTableClient = new TableClient(anonDriver);


        var user = $"test{Guid.NewGuid():n}";

        if (passwordCreate is null or "")
        {
            await Utils.ExecuteSchemeQuery(anonTableClient, $"CREATE USER {user}");
            _logger.LogInformation($"User {user} created successfully");
        }
        else
        {
            await Utils.ExecuteSchemeQuery(anonTableClient, $"CREATE USER {user} PASSWORD '{passwordCreate}'");
        }

        _logger.LogInformation($"User {user} with password {passwordCreate} created successfully");

        try
        {
            var driverConfig = new DriverConfig(
                endpoint: "grpc://localhost:2136",
                database: "/local",
                new StaticCredentialsProvider(user, passwordAuth) { MaxRetries = maxRetries }
            );

            _logger.LogInformation($"DriverConfig for {user} created");

            await using var driver = await Driver.CreateInitialized(driverConfig, _loggerFactory);
            _logger.LogInformation($"Driver for {user} created and initialized");
            using var tableClient = new TableClient(driver);

            var response = await Utils.ExecuteDataQuery(tableClient, "SELECT 1");
            var row = response.Result.ResultSets[0].Rows[0];
            Assert.Equal(1, row[0].GetInt32());
        }
        finally
        {
            await Utils.ExecuteSchemeQuery(anonTableClient, $"DROP USER {user}");
            _logger.LogInformation($"User {user} dropped succesfully");
        }
    }


    [Fact]
    public async Task GoodAuth()
    {
        await DoAuth("test_password", "test_password");
    }

    [Fact]
    public async Task NoPasswordAuth()
    {
        await DoAuth(null, null);
    }

    [Fact]
    public async Task WrongPassword()
    {
        await Assert.ThrowsAsync<InvalidCredentialsException>(
            async () => await DoAuth("good_password", "wrong_password", maxRetries: 1));
    }

    [Fact]
    public async Task NotExistAuth()
    {
        var driverConfig = new DriverConfig(
            endpoint: "grpc://localhost:2136",
            database: "/local",
            new StaticCredentialsProvider("notexists", "nopass") { MaxRetries = 1 }
        );

        await Assert.ThrowsAsync<InvalidCredentialsException>(
            async () => await Driver.CreateInitialized(driverConfig, _loggerFactory));
    }
}