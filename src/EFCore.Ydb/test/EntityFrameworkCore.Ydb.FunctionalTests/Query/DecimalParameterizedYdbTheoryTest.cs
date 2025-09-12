using EntityFrameworkCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class DecimalParameterizedYdbTheoryTest(DecimalParameterQueryYdbFixture fixture)
    : IClassFixture<DecimalParameterQueryYdbFixture>
{
    static DecimalParameterizedYdbTheoryTest()
        => AppContext.SetSwitch("EntityFrameworkCore.Ydb.EnableParametrizedDecimal", true);
    
    private DbContextOptions<ParametricDecimalContext> BuildOptions()
    {
        using var baseCtx = fixture.CreateContext();
        var cs = baseCtx.Database.GetDbConnection().ConnectionString;

        return new DbContextOptionsBuilder<ParametricDecimalContext>()
            .UseYdb(cs)
            .ReplaceService<IModelCacheKeyFactory, ParametricDecimalContext.CacheKeyFactory>()
            .Options;
    }

    public static IEnumerable<object[]> AdoLikeCases =>
    [
        [22, 9, 1.23456789m],
        [30, 10, 123.4567890123m],
        [18, 2, 12345678.91m],
        [10, 0, 9999999999m],
        [22, 9, -0.123456789m],
        [5, 2, 12.34m],
        [30, 10, 0.0000000001m]
    ];

    public static IEnumerable<object[]> OverflowCases =>
    [
        [15, 2, 123456789012345.67m],
        [10, 0, 12345678901m],
        [22, 9, 1.0000000001m],
        [18, 2, 1.239m],
        [18, 2, 100000000000000000m],
        [22, 9, 12345678901234567890.123456789m],
        [22, 9, -12345678901234567890.123456789m],
        [4, 2, 123.456m],
        [1, 0, 10m],
        [5, 0, 100000m]
    ];

    private ParametricDecimalContext NewCtx(int p, int s)
        => new(BuildOptions(), p, s);

    private static Task DropItemsTableAsync(DbContext ctx, int p, int s)
    {
        var helper = ctx.GetService<ISqlGenerationHelper>();
        var tableName = $"Items_{p}_{s}";
        var sql = $"DROP TABLE IF EXISTS {helper.DelimitIdentifier(tableName)}";

        return ctx.Database.ExecuteSqlRawAsync(sql);
    }

    [Theory]
    [MemberData(nameof(AdoLikeCases))]
    public async Task Decimal_roundtrips_or_rounds_like_ado(int p, int s, decimal value)
    {
        await using var ctx = NewCtx(p, s);
        await ctx.Database.EnsureCreatedAsync();

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
        finally
        {
            await DropItemsTableAsync(ctx, p, s);
        }
    }

    [Theory]
    [MemberData(nameof(OverflowCases))]
    public async Task Decimal_overflow_bubbles_up(int p, int s, decimal value)
    {
        AppContext.SetSwitch("EntityFrameworkCore.Ydb.EnableParametrizedDecimal", true);
        await using var ctx = NewCtx(p, s);
        await ctx.Database.EnsureCreatedAsync();

        try
        {
            ctx.Add(new ParamItem { Price = value });
            await Assert.ThrowsAsync<DbUpdateException>(() => ctx.SaveChangesAsync());
        }
        finally
        {
            await DropItemsTableAsync(ctx, p, s);
        }
    }
}
