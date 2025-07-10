using System.Data;
using AdoNet.Specification.Tests;
using Xunit;

namespace Ydb.Sdk.Ado.Specification.Tests;

public class YdbParameterTests(YdbFactoryFixture fixture) : ParameterTestBase<YdbFactoryFixture>(fixture)
{
#pragma warning disable xUnit1004
    [Fact(Skip = "DbType is Object how Npgsql")]
#pragma warning restore xUnit1004
    public override void Parameter_default_DbType_is_string()
    {
        base.Parameter_default_DbType_is_string();
    }

#pragma warning disable xUnit1004
    [Fact(Skip = "System.ArgumentNullException: Value cannot be null. (Parameter 'value')")]
#pragma warning restore xUnit1004
    public override void ParameterCollection_IndexOf_object_returns_negative_one_for_null()
    {
        base.ParameterCollection_IndexOf_object_returns_negative_one_for_null();
    }

    public override void ResetDbType_works()
    {
        var parameter = Fixture.Factory.CreateParameter();
        parameter!.DbType = DbType.Int64;

        parameter.ResetDbType();

        Assert.Equal(DbType.Object, parameter.DbType);
    }
}
