using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Ado;

internal interface IDriverFactory
{
    string GrpcConnectionString { get; }

    ILoggerFactory LoggerFactory { get; }
    Task<IDriver> CreateAsync();
}
