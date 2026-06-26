using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.BulkUpdates;
using Microsoft.EntityFrameworkCore.TestModels.InheritanceModel;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests.BulkUpdates;

public class TPHInheritanceBulkUpdatesYdbFixture : TPHInheritanceBulkUpdatesFixture
{
    protected override ITestStoreFactory TestStoreFactory
        => YdbTestStoreFactory.Instance;

    public override bool UseGeneratedKeys => false;

    protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
    {
        base.OnModelCreating(modelBuilder, context);

        // YDB requires a primary key for tables; map keyless query types to a view instead.
        modelBuilder
            .Entity<AnimalQuery>()
            .HasNoKey()
            .ToView(
                """
                SELECT * FROM `Animals`
                """
            );
    }
}
