using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

/// <summary>
/// Tests for loading many-to-many relationships in YDB provider.
/// </summary>
public class ManyToManyLoadYdbTest : ManyToManyLoadTestBase<ManyToManyLoadYdbTest.ManyToManyLoadYdbFixture>
{
    public ManyToManyLoadYdbTest(ManyToManyLoadYdbFixture fixture)
        : base(fixture)
    {
    }

    // YDB limitation: Some complex navigation scenarios may not be fully supported
    [ConditionalTheory(Skip = "YDB server limitation - complex query patterns")]
    [InlineData(true)]
    [InlineData(false)]
    public override Task Load_collection_using_Query_with_Include(bool async)
        => base.Load_collection_using_Query_with_Include(async);

    [ConditionalTheory(Skip = "YDB server limitation - complex query patterns")]
    [InlineData(true)]
    [InlineData(false)]
    public override Task Load_collection_using_Query_with_Include_for_inverse(bool async)
        => base.Load_collection_using_Query_with_Include_for_inverse(async);

    public class ManyToManyLoadYdbFixture : ManyToManyLoadFixtureBase
    {
        protected override string StoreName => "ManyToManyLoadYdbTest";

        protected override ITestStoreFactory TestStoreFactory
            => YdbTestStoreFactory.Instance;

        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            base.OnModelCreating(modelBuilder, context);

            // YDB-specific configuration for many-to-many relationships
            // Note: YDB may have limitations on complex join scenarios
        }
    }
}
