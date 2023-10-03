namespace Ydb.Sdk.Auth;

public interface IUseDriverConfig
{
    public Task ProvideConfig(DriverConfig driverConfig);
}