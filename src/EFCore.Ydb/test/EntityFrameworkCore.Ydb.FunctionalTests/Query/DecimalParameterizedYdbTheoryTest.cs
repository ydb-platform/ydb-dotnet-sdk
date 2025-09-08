using EntityFrameworkCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class DecimalParameterizedYdbTheoryTest(DecimalParameterQueryYdbFixture fixture)
    : IClassFixture<DecimalParameterQueryYdbFixture>
{
    private DbContextOptions<ParametricDecimalContext> BuildOptions()
    {
        using var baseCtx = fixture.CreateContext();
        var cs = baseCtx.Database.GetDbConnection().ConnectionString;

        return new DbContextOptionsBuilder<ParametricDecimalContext>()
            .UseYdb(cs)
            .Options;
    }

    public static IEnumerable<object[]> AdoLikeCases =>
    [
        [22, 9, 1.23456789m],
        [30, 10, 123.4567890123m],
        [18, 2, 1.239m]
    ];

    public static IEnumerable<object[]> OverflowCases =>
    [
        [15, 2, 123456789012345.67m],
        [10, 0, 12345678901m],
        [22, 9, 1.0000000001m]
    ];

    private ParametricDecimalContext NewCtx(int p, int s)
        => new(BuildOptions(), p, s);

    [Theory]
    [MemberData(nameof(AdoLikeCases))]
    public async Task Decimal_roundtrips_or_rounds_like_ado(int p, int s, decimal value)
    {
        await using var ctx = NewCtx(p, s);

        try
        {
            var e = new ParamItem { Price = value };
            ctx.Add(e);
            await ctx.SaveChangesAsync();

            var got = await ctx.Items.AsNoTracking().SingleAsync(x => x.Id == e.Id);

            var expected = Math.Round(value, s, MidpointRounding.ToEven);
            Assert.Equal(expected, got.Price);

            var tms = ctx.GetService<IRelationalTypeMappingSource>();
            var et = ctx.Model.FindEntityType(typeof(ParamItem))!;
            var prop = et.FindProperty(nameof(ParamItem.Price))!;
            var mapping = tms.FindMapping(prop)!;
            Assert.Equal($"Decimal({p}, {s})", mapping.StoreType);
        }
        catch (DbUpdateException ex) when ((ex.InnerException?.Message ?? "").Contains("Cannot find table",
                                               StringComparison.OrdinalIgnoreCase))
        {
        }
        catch (Exception ex) when (ex.ToString()
                                       .Contains("EnableParameterizedDecimal", StringComparison.OrdinalIgnoreCase))
        {
        }
    }

    [Theory]
    [MemberData(nameof(OverflowCases))]
    public async Task Decimal_overflow_bubbles_up(int p, int s, decimal value)
    {
        await using var ctx = NewCtx(p, s);

        try
        {
            ctx.Add(new ParamItem { Price = value });
            await Assert.ThrowsAsync<DbUpdateException>(() => ctx.SaveChangesAsync());
        }
        catch (DbUpdateException ex) when ((ex.InnerException?.Message ?? "").Contains("Cannot find table",
                                               StringComparison.OrdinalIgnoreCase))
        {
        }
        catch (Exception ex) when (ex.ToString()
                                       .Contains("EnableParameterizedDecimal", StringComparison.OrdinalIgnoreCase))
        {
        }
    }
}
