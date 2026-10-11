using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class ILikeParameterYdbTest
{
    [Fact]
    public async Task ILike_parameters_preserve_null_semantics()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create(nameof(ILikeParameterYdbTest));
        await using var context = new TestContext();
        await testStore.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();
        context.Entities.AddRange(
            new TestEntity { Id = 1, Name = "aLpHa" },
            new TestEntity { Id = 2, Name = "beta" },
            new TestEntity { Id = 3, Name = "100%" },
            new TestEntity { Id = 4 });
        await context.SaveChangesAsync();

        var pattern = new string?[] { null };
        var query = context.Entities.Where(e => EF.Functions.ILike(e.Name!, pattern[0]!));
        var negatedQuery = context.Entities.Where(e => !EF.Functions.ILike(e.Name!, pattern[0]!));
        Assert.Equal(0, await query.CountAsync());
        Assert.Equal(4, await negatedQuery.CountAsync());

        pattern[0] = "al%";
        Assert.Equal([1], await query.Select(e => e.Id).ToListAsync());
        Assert.Equal(3, await negatedQuery.CountAsync());

        pattern[0] = null;
        Assert.Empty(await query.ToListAsync());
        Assert.Equal(4, await negatedQuery.CountAsync());

        pattern[0] = "%";
        Assert.Equal(3, await query.CountAsync());
        Assert.Equal(1, await negatedQuery.CountAsync());

        pattern[0] = "al%";
        var matches = await context.Entities.OrderBy(e => e.Id)
            .Select(e => EF.Functions.ILike(e.Name!, pattern[0]!)).ToListAsync();
        Assert.Equal([true, false, false, false], matches);

        var match = new string?[] { null };
        match[0] = "ALPHA";
        var matchQuery = context.Entities
            .Where(e => e.Id == 1 && EF.Functions.ILike(match[0]!, "al%"))
            .Select(e => e.Id);
        Assert.Equal([1], await matchQuery.ToListAsync());
        match[0] = null;
        Assert.Empty(await matchQuery.ToListAsync());
        match[0] = "ALPHA";
        Assert.Equal([1], await matchQuery.ToListAsync());
    }

    public class TestEntity
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }

    public class TestContext : DbContext
    {
        public DbSet<TestEntity> Entities => Set<TestEntity>();

        protected override void OnConfiguring(DbContextOptionsBuilder options) =>
            options.UseYdb("Host=localhost;Port=2136");

        protected override void OnModelCreating(ModelBuilder model) =>
            model.Entity<TestEntity>(entity =>
            {
                entity.ToTable("ILikeEntities");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedNever();
            });
    }
}
