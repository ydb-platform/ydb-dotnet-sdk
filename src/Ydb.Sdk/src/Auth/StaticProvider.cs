using System.IdentityModel.Tokens.Jwt;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Ydb.Sdk.Services.Auth;

namespace Ydb.Sdk.Auth;

public class StaticProvider : IamProviderBase, IUseDriverConfig
{
    private readonly ILogger _logger;

    private readonly string _user;
    private readonly string? _password;

    private Driver? _driver;


    public StaticProvider(string user, string? password = null, ILoggerFactory? loggerFactory = null) : base(
        loggerFactory)
    {
        _user = user;
        _password = password;
        loggerFactory ??= NullLoggerFactory.Instance;
        _logger = loggerFactory.CreateLogger<StaticProvider>();
    }

    protected override async Task<IamTokenData> FetchToken()
    {
        if (_driver is null)
        {
            _logger.LogError("Driver in for static auth not provided");
            throw new NullReferenceException();
        }

        var client = new AuthClient(_driver);
        var loginResponse = await client.Login(_user, _password);
        loginResponse.Status.EnsureSuccess();
        var token = loginResponse.Result.Token;
        var jwt = new JwtSecurityToken(token);
        return new IamTokenData(token, jwt.ValidTo);
    }

    public async Task ProvideConfig(DriverConfig driverConfig)
    {
        _driver = await Driver.CreateInitialized(
            new DriverConfig(
                driverConfig.Endpoint,
                driverConfig.Database,
                new AnonymousProvider(),
                driverConfig.DefaultTransportTimeout,
                driverConfig.DefaultStreamingTransportTimeout,
                driverConfig.CustomServerCertificate));
    }
}