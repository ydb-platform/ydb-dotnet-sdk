namespace Ydb.Sdk.Auth;

public interface ICredentialsProvider
{
    string? GetAuthInfo();

    Task ProvideConfig(DriverConfig driverConfig) => Task.CompletedTask;
}
