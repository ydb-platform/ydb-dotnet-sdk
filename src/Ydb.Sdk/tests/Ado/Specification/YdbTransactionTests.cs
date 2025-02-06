using AdoNet.Specification.Tests;

namespace Ydb.Sdk.Tests.Ado.Specification;

public class YdbTransactionTests : TransactionTestBase<YdbFactoryFixture>
{
    public YdbTransactionTests(YdbFactoryFixture fixture) : base(fixture)
    {
    }
}
