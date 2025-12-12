using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;

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
    [ConditionalTheory(Skip = "YDB server limitation - complex inheritance patterns")]
    [InlineData(true)]
    [InlineData(false)]
    public override Task Can_insert_many_to_many_with_inheritance(bool async)
        => base.Can_insert_many_to_many_with_inheritance(async);

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
