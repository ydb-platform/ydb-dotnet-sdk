using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestModels.InheritanceModel;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class TphInheritanceQueryYdbFixture : TPHInheritanceQueryFixture
{
    protected override ITestStoreFactory TestStoreFactory
        => YdbTestStoreFactory.Instance;

    protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
    {
        base.OnModelCreating(modelBuilder, context);

        // TODO: For unknown reason ToSqlQuery doesn't work
        // Better to use it but ToView is pretty good replacement
        modelBuilder
            .Entity<AnimalQuery>()
            .HasNoKey()
            .ToView(
                """
                SELECT * FROM "Animals"
                """
            );
    }
}
