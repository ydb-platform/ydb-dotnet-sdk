using Microsoft.EntityFrameworkCore;
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

        decimal v = 1.23456789m;
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

        decimal v = 123.456789012m;
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

        var tooBig = new ItemExplicit { Price = 10_000_000_000_000m }; // 14 целых цифр -> вне 22,9
        ctx.Add(tooBig);

        var ex = await Assert.ThrowsAsync<DbUpdateException>(() => ctx.SaveChangesAsync());
        Assert.Contains("Decimal", ex.InnerException?.Message ?? "");
    }
}
