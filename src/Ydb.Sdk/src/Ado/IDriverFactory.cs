using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Ado;

internal interface IDriverFactory
{
    Task<IDriver> CreateAsync();

    string GrpcConnectionString { get; }

    ILoggerFactory LoggerFactory { get; }
}
