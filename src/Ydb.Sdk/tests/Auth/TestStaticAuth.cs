using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Xunit.Abstractions;
using Ydb.Sdk.Auth;
using Ydb.Sdk.Services.Table;

namespace Ydb.Sdk.Tests.Auth;

[Trait("Category", "Integration")]
[Collection("TODO")]
public class TestStaticAuth : IDisposable
{
    // ReSharper disable once NotAccessedField.Local
    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory? _loggerFactory;
    private readonly ILogger _logger;

    public TestStaticAuth(ITestOutputHelper output)
    {
        _output = output;
        _loggerFactory = Utils.GetLoggerFactory() ?? NullLoggerFactory.Instance;
        _logger = _loggerFactory.CreateLogger<TestStaticAuth>();
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
    }

    private async Task CreateUser(TableClient tableClient, string user, string? password)
    {
        var query = password is null ? $"CREATE USER {user}" : $"CREATE USER {user} PASSWORD '{password}'";

        await Utils.ExecuteSchemeQuery(tableClient, $"{query}");
        _logger.LogInformation($"User {user} created successfully");
    }

    private async Task DropUser(TableClient tableClient, string user)
    {
        await Utils.ExecuteSchemeQuery(tableClient, $"DROP USER {user}");
        _logger.LogInformation($"User {user} dropped successfully");
    }

    private async Task Authorize(string user, string? password, int maxRetries)
    {
        var driverConfig = new DriverConfig(
            endpoint: "grpc://localhost:2136",
            database: "/local",
            new StaticCredentialsProvider(user, password, _loggerFactory) { MaxRetries = maxRetries }
        );

        _logger.LogInformation($"DriverConfig for {user} created");

        await using var driver = await Driver.CreateInitialized(driverConfig, _loggerFactory);
        _logger.LogInformation($"Driver for {user} created and initialized");
        using var tableClient = new TableClient(driver);

        var response = await Utils.ExecuteDataQuery(tableClient, "SELECT 1");
        var row = response.Result.ResultSets[0].Rows[0];
        Assert.Equal(1, row[0].GetInt32());
    }

    private async Task CheckAuth(string? passwordCreate, string? passwordAuth, int maxRetries = 5)
    {
        _logger.LogInformation("Creating anon driver");
        var anonDriverConfig = new DriverConfig(
            endpoint: "grpc://localhost:2136",
            database: "/local"
        );

        await using var anonDriver = await Driver.CreateInitialized(anonDriverConfig);
        _logger.LogInformation("Anon driver created");

        using var anonTableClient = new TableClient(anonDriver);

        var user = $"test{Guid.NewGuid():n}";

        await CreateUser(anonTableClient, user, passwordCreate);

        try
        {
            await Authorize(user, passwordAuth, maxRetries);
        }
        finally
        {
            await DropUser(anonTableClient, user);
        }
    }


    [Fact]
    public async Task GoodAuth()
    {
        await CheckAuth("test_password", "test_password");
    }

    [Fact]
    public async Task NoPasswordAuth()
    {
        await CheckAuth(null, null);
    }

    [Fact]
    public async Task WrongPassword()
    {
        await Assert.ThrowsAsync<InvalidCredentialsException>(
            async () => await CheckAuth("good_password", "wrong_password", maxRetries: 1));
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
