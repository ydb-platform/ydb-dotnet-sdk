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


        _output = output;

        _logger.LogInformation("Creating anon driver");
        var driverConfig = new DriverConfig(
            endpoint: "grpc://localhost:2136",
            database: "/local"
        );

        _anonDriver = new Driver(driverConfig);
        _anonDriver.Initialize().Wait();
        _logger.LogInformation("Anon driver created");

        _anonTableClient = new TableClient(_anonDriver);
    }

    public void Dispose()
    {
        _anonTableClient.Dispose();
        _anonDriver.Dispose();
        GC.SuppressFinalize(this);
    }

    private async Task CreateUser(string user, string? password)
    {
        var query = password is null ? $"CREATE USER {user}" : $"CREATE USER {user} PASSWORD '{password}'";

        await Utils.ExecuteSchemeQuery(_anonTableClient, $"{query}");
        _logger.LogInformation($"User {user} created successfully");
    }

    private async Task DropUser(string user)
    {
        await Utils.ExecuteSchemeQuery(_anonTableClient, $"DROP USER {user}");
        _logger.LogInformation($"User {user} dropped successfully");
    }

    private async Task DoAuth(string? passwordCreate, string? passwordAuth, int maxRetries = 5)
    {
        var user = $"test{Guid.NewGuid():n}";

        await CreateUser(user, passwordCreate);

        try
        {
            var driverConfig = new DriverConfig(
                endpoint: "grpc://localhost:2136",
                database: "/local",
                new StaticCredentialsProvider(user, passwordAuth, _loggerFactory) { MaxRetries = maxRetries }
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
            await DropUser(user);
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
            new StaticCredentialsProvider("notexists", "nopass", _loggerFactory) { MaxRetries = 1 }
        );

        await Assert.ThrowsAsync<InvalidCredentialsException>(
            async () => await Driver.CreateInitialized(driverConfig, _loggerFactory));
    }
}