#if !EFCORE9
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query.Translations;
using Microsoft.EntityFrameworkCore.TestModels.BasicTypesModel;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query.Translations;

public class BasicTypesQueryYdbFixture : BasicTypesQueryFixtureBase
{
    protected override ITestStoreFactory TestStoreFactory
        => YdbTestStoreFactory.Instance;

    protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
    {
        modelBuilder.Entity<BasicTypesEntity>(entity =>
        {
            entity.Property(e => e.DateOnly).HasColumnType("Date32");
            entity.Property(e => e.DateTime).HasColumnType("Timestamp64");
            entity.Property(e => e.TimeOnly).HasConversion<long>();
            entity.Property(e => e.TimeSpan).HasConversion<long>();
            entity.Property(e => e.DateTimeOffset).HasConversion<string>();
        });

        modelBuilder.Entity<NullableBasicTypesEntity>(entity =>
        {
            entity.Property(e => e.DateOnly).HasColumnType("Date32");
            entity.Property(e => e.DateTime).HasColumnType("Timestamp64");
            entity.Property(e => e.TimeOnly).HasConversion<long>();
            entity.Property(e => e.TimeSpan).HasConversion<long>();
            entity.Property(e => e.DateTimeOffset).HasConversion<string>();
        });
    }
}
#endif
