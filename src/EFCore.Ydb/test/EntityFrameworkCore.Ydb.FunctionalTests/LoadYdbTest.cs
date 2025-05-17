using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public class LoadYdbTest : LoadTestBase<LoadYdbTest.LoadYdbFixture>
{
    public LoadYdbTest(LoadYdbFixture fixture)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
    }

    protected override void ClearLog()
        => Fixture.TestSqlLoggerFactory.Clear();

    protected override void RecordLog()
        => Sql = Fixture.TestSqlLoggerFactory.Sql;

    private string Sql { get; set; } = null!;

    public class LoadYdbFixture : LoadFixtureBase
    {
        public TestSqlLoggerFactory TestSqlLoggerFactory
            => (TestSqlLoggerFactory)ServiceProvider.GetRequiredService<ILoggerFactory>();

        protected override ITestStoreFactory TestStoreFactory
            => YdbTestStoreFactory.Instance;

        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            base.OnModelCreating(modelBuilder, context);

            modelBuilder
                .Entity<OptionalChildView>()
                .ToView("OptionalChildView");
            modelBuilder
                .Entity<RequiredChildView>()
                .ToView("RequiredChildView");
        }
    }
}
