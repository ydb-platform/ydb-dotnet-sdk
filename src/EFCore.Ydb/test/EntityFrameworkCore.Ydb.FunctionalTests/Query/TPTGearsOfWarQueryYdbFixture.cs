using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestModels.GearsOfWarModel;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class TptGearsOfWarQueryYdbFixture: TPTGearsOfWarQueryRelationalFixture
{
    protected override ITestStoreFactory TestStoreFactory
        => YdbTestStoreFactory.Instance;

    protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
    {
        base.OnModelCreating(modelBuilder, context);

        modelBuilder.Entity<Mission>()
            .Property(e => e.Time)
            .HasConversion(
                v => new DateTime(2000, 1, 1).Add(v.ToTimeSpan()), // Time → DateTime
                v => new TimeOnly(v.TimeOfDay.Ticks)) // DateTime → Time
            .HasColumnType("DATETIME");

        modelBuilder.Entity<Mission>()
            .Property(e => e.Date)
            .HasConversion(
                v => new DateTime(v.Year, v.Month, v.Day), // DateOnly → DateTime
                v => new DateOnly(v.Year, v.Month, v.Day)) // DateTime → DateOnly
            .HasColumnType("DATETIME");
    }
}
