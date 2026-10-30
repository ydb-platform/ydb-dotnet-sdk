using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class DefaultIfEmptyYdbTest
{
    [Fact]
    public async Task DefaultIfEmpty_preserves_rows_nulls_and_parameter_changes()
    {
        await using var store = YdbTestStoreFactory.Instance.Create(nameof(DefaultIfEmptyYdbTest));
        await using var context = new TestContext();
        await store.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();
        context.Entities.AddRange(
            new TestEntity { Id = 0, Key = 0 },
            new TestEntity { Id = 1, Key = 7 },
            new TestEntity { Id = 2, Key = 7 },
            new TestEntity { Id = 3 });
        await context.SaveChangesAsync();

        var entities = context.Entities;
        var source = await entities.OrderBy(e => e.Id).Select(e => new { e.Id, e.Key }).ToArrayAsync();
        Assert.Equal([(0, (int?)0), (1, 7), (2, 7), (3, null)], source.Select(e => (e.Id, e.Key)));

        var ids = await entities.DefaultIfEmpty().Select(e => e == null ? (int?)null : e.Id)
            .OrderBy(id => id).ToArrayAsync();
        Assert.Equal([0, 1, 2, 3], ids);

        var keys = await entities.Where(e => e.Key == 7).Select(e => e.Key).DefaultIfEmpty().ToArrayAsync();
        Assert.Equal([7, 7], keys);

        var key = new int?[] { 7 };
        var query = entities.Where(e => e.Key == key[0]).DefaultIfEmpty()
            .Select(e => e == null ? (int?)null : e.Id).OrderBy(id => id);
        Assert.Equal([1, 2], await query.ToArrayAsync());
        key[0] = 99;
        Assert.Equal([null], await query.ToArrayAsync());
        key[0] = null;
        Assert.Equal([3], await query.ToArrayAsync());
    }

    public class TestEntity
    {
        public int Id { get; set; }
        public int? Key { get; set; }
    }

    public class TestContext : DbContext
    {
        public DbSet<TestEntity> Entities => Set<TestEntity>();

        protected override void OnConfiguring(DbContextOptionsBuilder options) =>
            options.UseYdb("Host=localhost;Port=2136");

        protected override void OnModelCreating(ModelBuilder model) => model.Entity<TestEntity>(entity =>
        {
            entity.ToTable("DefaultIfEmptyEntities");
            entity.Property(e => e.Id).HasColumnName("empty_key").ValueGeneratedNever();
        });
    }
}
