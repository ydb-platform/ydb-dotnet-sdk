using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.BulkUpdates;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.TestModels.Northwind;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests.BulkUpdates;

public class NorthwindBulkUpdatesYdbFixture<TModelCustomizer> :
    NorthwindBulkUpdatesRelationalFixture<TModelCustomizer>
    where TModelCustomizer : ITestModelCustomizer, new()
{
    protected override ITestStoreFactory TestStoreFactory => YdbNorthwindTestStoreFactory.Instance;

    protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
    {
        base.OnModelCreating(modelBuilder, context);
        modelBuilder.Entity<OrderDetail>().ToTable("Order_Details");
    }
}
