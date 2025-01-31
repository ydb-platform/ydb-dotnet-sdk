using AdoNet.Specification.Tests;

namespace Ydb.Sdk.Tests.Ado.Specification;

public class YdbConnectionStringBuilderTests : ConnectionStringTestBase<YdbFactoryFixture>
{
    public YdbConnectionStringBuilderTests(YdbFactoryFixture fixture) : base(fixture)
    {
    }
}
