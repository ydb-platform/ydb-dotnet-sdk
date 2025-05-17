using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestModels.GearsOfWarModel;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class GearsOfWarQueryYdbFixture : GearsOfWarQueryRelationalFixture
{
    protected override string StoreName
        => "GearsOfWarQueryTest";

    protected override ITestStoreFactory TestStoreFactory
        => YdbTestStoreFactory.Instance;

    protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
    {
        base.OnModelCreating(modelBuilder, context);

        modelBuilder.Entity<Mission>()
            .Property(e => e.Date)
            .HasConversion(
                v => v.ToDateTime(TimeOnly.MinValue),
                v => DateOnly.FromDateTime(v));

        modelBuilder.Entity<Mission>()
            .Property(e => e.Duration)
            .HasConversion(
                v => new DateTime(1970, 1, 1).Add(v),
                v => v.TimeOfDay);
    }
}
