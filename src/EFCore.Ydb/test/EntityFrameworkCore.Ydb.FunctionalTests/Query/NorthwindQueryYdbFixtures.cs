using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestModels.Northwind;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class NorthwindQueryYdbFixture<TModelCustomizer> : NorthwindQueryRelationalFixture<TModelCustomizer>
    where TModelCustomizer : ITestModelCustomizer, new()
{
    protected override ITestStoreFactory TestStoreFactory
        => YdbNorthwindTestStoreFactory.Instance;

    protected override Type ContextType
        => typeof(NorthwindContext);

    protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
    {
        base.OnModelCreating(modelBuilder, context);

        modelBuilder.Entity<OrderDetail>().ToTable("Order_Details");
        modelBuilder.Entity<Employee>().Property(e => e.EmployeeID).HasConversion<int>();
        modelBuilder.Entity<Employee>().Property(e => e.ReportsTo).HasConversion<int>();
        modelBuilder.Entity<Order>().Property(e => e.EmployeeID).HasConversion<int>();
        modelBuilder.Entity<Product>().Property(e => e.UnitsInStock).HasConversion<int>();
        modelBuilder.Entity<Product>().Property(e => e.UnitsOnOrder).HasConversion<int>();
        modelBuilder.Entity<Product>().Property(e => e.ReorderLevel).HasConversion<int>();
    }
}
