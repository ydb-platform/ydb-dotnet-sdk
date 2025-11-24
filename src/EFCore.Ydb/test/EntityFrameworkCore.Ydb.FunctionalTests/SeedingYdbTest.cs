using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

/// <summary>
/// Tests for database seeding with HasData in YDB provider.
/// Note: Some seeding tests are skipped due to YDB-specific limitations.
/// </summary>
public class SeedingYdbTest : SeedingTestBase
{
    protected override TestStore TestStore => YdbTestStore.GetOrCreate("SeedingTest");

    protected override SeedingContext CreateContextWithEmptyDatabase(string testId)
        => new YdbSeedingContext(testId, ((YdbTestStore)TestStore).ConnectionString);

    protected class YdbSeedingContext(string testId, string connectionString) : SeedingContext(testId)
    {
        private readonly string _connectionString = connectionString;

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseYdb(_connectionString);
        }
    }
}
