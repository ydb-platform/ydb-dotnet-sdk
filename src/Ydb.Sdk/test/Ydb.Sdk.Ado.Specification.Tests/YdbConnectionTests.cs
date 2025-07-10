using AdoNet.Specification.Tests;
using Xunit;

namespace Ydb.Sdk.Ado.Specification.Tests;

public class YdbConnectionTests(YdbFactoryFixture fixture) : ConnectionTestBase<YdbFactoryFixture>(fixture)
{
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
    [Fact(Skip = "TODO Supported this field.")]
#pragma warning restore xUnit1004
    public override void ServerVersion_returns_value()
    {
        base.ServerVersion_returns_value();
    }

    public override async Task OpenAsync_is_canceled()
    {
        await using var connection = CreateConnection();
        connection.ConnectionString = ConnectionString;
        var task = connection.OpenAsync(CanceledToken);
        await Assert.ThrowsAnyAsync<YdbException>(() => task);
        Assert.True(task.IsFaulted);
    }
}
