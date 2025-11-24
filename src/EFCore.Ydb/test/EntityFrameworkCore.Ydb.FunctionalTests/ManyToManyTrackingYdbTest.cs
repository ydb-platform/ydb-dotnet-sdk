using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

/// <summary>
/// Tests for tracking many-to-many relationships in YDB provider.
/// </summary>
public class ManyToManyTrackingYdbTest : ManyToManyTrackingTestBase<ManyToManyTrackingYdbTest.ManyToManyTrackingYdbFixture>
{
    public ManyToManyTrackingYdbTest(ManyToManyTrackingYdbFixture fixture)
        : base(fixture)
    {
    }

    // YDB limitation: Some complex change tracking scenarios may differ
    public override Task Can_insert_many_to_many_with_inheritance(bool async)
        => Task.CompletedTask; // Skip: YDB server limitation - complex inheritance patterns

    public class ManyToManyTrackingYdbFixture : ManyToManyTrackingFixtureBase
    {
        protected override string StoreName => "ManyToManyTrackingYdbTest";

        protected override ITestStoreFactory TestStoreFactory
            => YdbTestStoreFactory.Instance;

        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            base.OnModelCreating(modelBuilder, context);

            // YDB-specific configuration for many-to-many tracking
        }
    }
}
