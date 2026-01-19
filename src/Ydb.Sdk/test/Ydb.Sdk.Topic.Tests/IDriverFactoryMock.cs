using Microsoft.Extensions.Logging;
using Moq;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Topic.Tests;

public class IDriverFactoryMock(Mock<IDriver> mockIDriver, string grpcConnectionString) : IDriverFactory
{
    public Task<IDriver> CreateAsync() => Task.FromResult(mockIDriver.Object);

    public string GrpcConnectionString => grpcConnectionString;

    public ILoggerFactory LoggerFactory => Utils.LoggerFactory;
}
