namespace Ydb.Sdk.Ado;

internal interface  IDriverFactory
{
    Task<IDriver> CreateAsync();
    
    string GrpcConnectionString { get; }
}
