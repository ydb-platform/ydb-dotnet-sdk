using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public class LazyLoadProxyYdbTest(LazyLoadProxyYdbTest.LoadYdbFixture fixture)
    : LazyLoadProxyTestBase<LazyLoadProxyYdbTest.LoadYdbFixture>(fixture)
{
    [ConditionalFact(Skip = "TODO: Fix precision")]
    public override void Can_serialize_proxies_to_JSON() => base.Can_serialize_proxies_to_JSON();

    [ConditionalTheory(Skip = "TODO: InvalidOperationException")]
    [InlineData(EntityState.Unchanged)]
    public override void Lazy_load_one_to_one_reference_with_recursive_property(EntityState state) =>
        base.Lazy_load_one_to_one_reference_with_recursive_property(state);

    [ConditionalFact(Skip = "TODO: InvalidOperationException")]
    public override void Top_level_projection_track_entities_before_passing_to_client_method() =>
        base.Top_level_projection_track_entities_before_passing_to_client_method();

    public class LoadYdbFixture : LoadFixtureBase
    {
        public TestSqlLoggerFactory TestSqlLoggerFactory
            => (TestSqlLoggerFactory)ServiceProvider.GetRequiredService<ILoggerFactory>();

        protected override ITestStoreFactory TestStoreFactory
            => YdbTestStoreFactory.Instance;

        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            base.OnModelCreating(modelBuilder, context);

            modelBuilder.Entity<Parson>()
                .Property(e => e.Birthday)
                .HasColumnType("Timestamp64");
            
            modelBuilder.Entity<Quest>()
                .Property(e => e.Birthday)
                .HasColumnType("Timestamp64");
        }
    }
}
