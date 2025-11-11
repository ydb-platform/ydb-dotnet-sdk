using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestModels.GearsOfWarModel;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class TPTGearsOfWarQueryYdbFixture : TPTGearsOfWarQueryRelationalFixture
{
    protected override ITestStoreFactory TestStoreFactory
        => YdbTestStoreFactory.Instance;

    protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
    {
        base.OnModelCreating(modelBuilder, context);

        modelBuilder.Entity<Mission>()
            .Property(e => e.Date)
            .HasColumnType("Date32");

        modelBuilder.Entity<Mission>()
            .Property(e => e.Time)
            .HasColumnType("Datetime64")
            .HasConversion(
                v => new DateTime(2000, 1, 1).Add(v.ToTimeSpan()), // Time → DateTime
                v => new TimeOnly(v.TimeOfDay.Ticks)
            ); // DateTime → Time
                
        modelBuilder.Entity<CogTag>()
            .Property(e => e.IssueDate)
            .HasColumnType("Date32");
    }
}
