using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using EntityFrameworkCore.Ydb.Infrastructure;
using EntityFrameworkCore.Ydb.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

/// <summary>
/// Tests for transaction support in YDB provider.
/// Note: YDB has specific transaction semantics that differ from traditional RDBMS.
/// </summary>
public class TransactionYdbTest(TransactionYdbTest.TransactionYdbFixture fixture)
    : TransactionTestBase<TransactionYdbTest.TransactionYdbFixture>(fixture)
{
    protected override bool SnapshotSupported => false; // YDB server limitation

    // YDB limitation: Nested transactions and savepoints work differently
    [ConditionalTheory(Skip = "YDB server limitation - savepoint semantics differ")]
    [InlineData(true)]
    [InlineData(false)]
    public override Task SaveChanges_can_be_used_with_no_savepoint(bool async)
        => base.SaveChanges_can_be_used_with_no_savepoint(async);

    [ConditionalTheory(Skip = "YDB server limitation - batching creates implicit transactions")]
    [InlineData(true)]
    [InlineData(false)]
    public override Task SaveChanges_can_be_used_with_AutoTransactionBehavior_Never(bool async)
        => base.SaveChanges_can_be_used_with_AutoTransactionBehavior_Never(async);

#pragma warning disable CS0618 // AutoTransactionsEnabled is obsolete
    [ConditionalTheory(Skip = "YDB server limitation - batching creates implicit transactions")]
    [InlineData(true)]
    [InlineData(false)]
    public override Task SaveChanges_can_be_used_with_AutoTransactionsEnabled_false(bool async)
        => base.SaveChanges_can_be_used_with_AutoTransactionsEnabled_false(async);
#pragma warning restore CS0618

    protected override DbContext CreateContextWithConnectionString()
    {
        var options = Fixture.AddOptions(
                new DbContextOptionsBuilder()
                    .UseYdb(TestStore.ConnectionString))
            .UseInternalServiceProvider(Fixture.ServiceProvider);

        return new DbContext(options.Options);
    }

    public class TransactionYdbFixture : TransactionFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory => YdbTestStoreFactory.Instance;
    }
}
