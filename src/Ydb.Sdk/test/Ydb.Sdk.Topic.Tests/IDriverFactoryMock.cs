using Microsoft.Extensions.Logging;
using Moq;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Topic.Tests;

internal class IDriverFactoryMock(
    Mock<IDriver> mockIDriver,
    string grpcConnectionString,
    string endpoint = "localhost:2136",
    string database = "/local") : IDriverFactory
{
    public Task<IDriver> CreateAsync() => Task.FromResult(mockIDriver.Object);

    public string Endpoint => endpoint;

    public string Database => database;

    public string GrpcConnectionString => grpcConnectionString;

    public ILoggerFactory LoggerFactory => Utils.LoggerFactory;
}
