using AdoNet.Specification.Tests;
using Xunit;

namespace Ydb.Sdk.Tests.Ado.Specification;

public class YdbConnectionTests : ConnectionTestBase<YdbFactoryFixture>
{
    public YdbConnectionTests(YdbFactoryFixture fixture) : base(fixture)
    {
    }

#pragma warning disable xUnit1004
    [Fact(Skip = "IComponent legacy.")]
#pragma warning restore xUnit1004
    public override void Dispose_raises_Disposed()
    {
        base.Dispose_raises_Disposed();
    }

#pragma warning disable xUnit1004
    [Fact(Skip = "IComponent legacy.")]
#pragma warning restore xUnit1004
    public override Task DisposeAsync_raises_Disposed()
    {
        return base.DisposeAsync_raises_Disposed();
    }

#pragma warning disable xUnit1004
    [Fact(Skip = "Connect to default settings 'grpc://localhost:2136/local'.")]
#pragma warning restore xUnit1004
    public override void Open_throws_when_no_connection_string()
    {
        base.Open_throws_when_no_connection_string();
    }

#pragma warning disable xUnit1004
    [Fact(Skip = "TODO Supported this field.")]
#pragma warning restore xUnit1004
    public override void ServerVersion_returns_value()
    {
        base.ServerVersion_returns_value();
    }

#pragma warning disable xUnit1004
    [Fact(Skip = "TODO Supported cancel OpenAsync.")]
#pragma warning restore xUnit1004
    public override Task OpenAsync_is_canceled()
    {
        return base.OpenAsync_is_canceled();
    }
}
