using AdoNet.Specification.Tests;
using Xunit;

namespace Ydb.Sdk.Tests.Ado.Specification;

public class YdbProviderFactoryTests : DbProviderFactoryTestBase<YdbFactoryFixture>
{
    public YdbProviderFactoryTests(YdbFactoryFixture fixture) : base(fixture)
    {
    }

#pragma warning disable xUnit1004
    [Fact(Skip = "DbCommandBuilder isn't supported")]
#pragma warning restore xUnit1004
    public override void DbProviderFactory_CanCreateCommandBuilder_is_true()
    {
        Assert.True(Fixture.Factory.CanCreateCommandBuilder);
    }

#pragma warning disable xUnit1004
    [Fact(Skip = "DataAdapter isn't supported")]
#pragma warning restore xUnit1004
    public override void DbProviderFactory_CanCreateDataAdapter_is_true()
    {
        Assert.True(Fixture.Factory.CanCreateDataAdapter);
    }

#pragma warning disable xUnit1004
    [Fact(Skip = "DbCommandBuilder isn't supported")]
#pragma warning restore xUnit1004
    public override void DbProviderFactory_CreateCommandBuilder_is_not_null()
    {
        base.DbProviderFactory_CreateCommandBuilder_is_not_null();
    }

#pragma warning disable xUnit1004
    [Fact(Skip = "DataAdapter isn't supported")]
#pragma warning restore xUnit1004
    public override void DbProviderFactory_CreateDataAdapter_is_not_null()
    {
        base.DbProviderFactory_CreateDataAdapter_is_not_null();
    }
}
