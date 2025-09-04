using EntityFrameworkCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class DecimalParameterYdbTest(DecimalParameterQueryYdbFixture fixture)
    : IClassFixture<DecimalParameterQueryYdbFixture>
{
    private DecimalParameterQueryYdbFixture Fixture { get; } = fixture;

    [ConditionalFact]
    public async Task Parameter_decimal_uses_default_22_9_and_roundtrips()
    {
        await using var ctx = Fixture.CreateContext();
        await ctx.Database.EnsureCreatedAsync();

        var v = 1.23456789m;
        ctx.Add(new ItemDefault { Price = v });
        await ctx.SaveChangesAsync();

        var got = await ctx.Set<ItemDefault>().Where(x => x.Price == v).ToListAsync();
        Assert.Single(got);
        Assert.Equal(v, got[0].Price);
    }

    [ConditionalFact]
    public async Task Parameter_decimal_respects_explicit_22_9_and_roundtrips()
    {
        await using var ctx = Fixture.CreateContext();
        await ctx.Database.EnsureCreatedAsync();

        var v = 123.456789012m;
        ctx.Add(new ItemExplicit { Price = v });
        await ctx.SaveChangesAsync();

        var got = await ctx.Set<ItemExplicit>().Where(x => x.Price == v).ToListAsync();
        Assert.Single(got);
        Assert.Equal(v, got[0].Price);
    }

    [ConditionalFact]
    public async Task Decimal_out_of_range_bubbles_up()
    {
        await using var ctx = Fixture.CreateContext();
        await ctx.Database.EnsureCreatedAsync();

        var tooBig = new ItemExplicit { Price = 10_000_000_000_000m };
        ctx.Add(tooBig);

        var ex = await Assert.ThrowsAsync<DbUpdateException>(() => ctx.SaveChangesAsync());
        Assert.Contains("Decimal", ex.InnerException?.Message ?? "");
    }

    [Fact]
    public void Type_mapping_default_decimal_is_22_9()
    {
        using var ctx = Fixture.CreateContext();
        var tms = ctx.GetService<IRelationalTypeMappingSource>();
        var mapping = tms.FindMapping(typeof(decimal))!;
        Assert.Equal("Decimal(22, 9)", mapping.StoreType);
    }

    [Fact]
    public void Type_mapping_custom_decimal_is_30_10()
    {
        var opts = new DbContextOptionsBuilder<MappingOnlyContext>()
            .UseYdb("")
            .Options;

        using var ctx = new MappingOnlyContext(opts);
        var tms = ctx.GetService<IRelationalTypeMappingSource>();

        var et = ctx.Model.FindEntityType(typeof(MappingEntity))!;
        var prop = et.FindProperty(nameof(MappingEntity.Price))!;
        var mapping = tms.FindMapping(prop)!;

        Assert.Equal("Decimal(30, 10)", mapping.StoreType);
    }

    private sealed class MappingOnlyContext(DbContextOptions<MappingOnlyContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder b)
            => b.Entity<MappingEntity>(e =>
               {
                   e.HasKey(x => x.Id);
                   e.Property(x => x.Price).HasPrecision(30, 10);
               });
    }

    private sealed class MappingEntity
    {
        public int Id { get; set; }
        public decimal Price { get; set; }
    }
}
