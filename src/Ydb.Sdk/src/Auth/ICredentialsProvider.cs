namespace Ydb.Sdk.Auth;

public interface ICredentialsProvider
{
    string? GetAuthInfo();
    
    void ProvideConfig(DriverConfig driverConfig) {}
}
