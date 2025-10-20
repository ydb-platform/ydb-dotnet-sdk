using EntityFrameworkCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class DecimalParameterizedYdbTest
{
    private static DbContextOptions<ParametricDecimalContext> BuildOptions() =>
        new DbContextOptionsBuilder<ParametricDecimalContext>()
            .UseYdb("Host=localhost;Port=2136")
            .EnableServiceProviderCaching(false)
            .LogTo(Console.WriteLine)
            .Options;

    public static TheoryData<int, int, decimal> SuccessCases => new()
    {
        { 22, 9, 1.23456789m },
        { 30, 10, 123.4567890123m },
        { 18, 2, 12345678.91m },
        { 10, 0, 9999999999m },
        { 22, 9, -0.123456789m },
        { 5, 2, 12.34m },
        { 30, 10, 0.0000000001m }
    };

    public static TheoryData<int, int, decimal> OverflowCases => new()
    {
        { 15, 2, 123456789012345.67m },
        { 10, 0, 12345678901m },
        { 22, 9, 1.0000000001m },
        { 18, 2, 1.239m },
        { 18, 2, 100000000000000000m },
        { 22, 9, 12345678901234567890.123456789m },
        { 22, 9, -12345678901234567890.123456789m },
        { 4, 2, 123.456m },
        { 1, 0, 10m },
        { 5, 0, 100000m }
    };

    private static ParametricDecimalContext NewCtx(int p, int s) => new(BuildOptions(), p, s);

    [Theory]
    [MemberData(nameof(SuccessCases))]
    public async Task Should_RoundtripDecimal_When_ValueFitsPrecisionAndScale(int p, int s, decimal value)
    {
        await using var ctx = NewCtx(p, s);
        await ctx.Database.EnsureCreatedAsync();
        try
        {
            var e = new ParamItem { Price = value };
            ctx.Add(e);
            await ctx.SaveChangesAsync();
            var got = await ctx.Items.SingleAsync(x => x.Id == e.Id);
            Assert.Equal(value, got.Price);
            var tms = ctx.GetService<IRelationalTypeMappingSource>();
            var et = ctx.Model.FindEntityType(typeof(ParamItem))!;
            var prop = et.FindProperty(nameof(ParamItem.Price))!;
            var mapping = tms.FindMapping(prop)!;
            Assert.Equal($"Decimal({p}, {s})", mapping.StoreType);
        }
        finally
        {
            await ctx.Database.EnsureDeletedAsync();
        }
    }

    [Theory]
    [MemberData(nameof(OverflowCases))]
    public async Task Should_ThrowOverflow_When_ValueExceedsPrecisionOrScale(int p, int s, decimal value)
    {
        await using var ctx = NewCtx(p, s);
        await ctx.Database.EnsureCreatedAsync();
        try
        {
            ctx.Add(new ParamItem { Price = value });
            await Assert.ThrowsAsync<DbUpdateException>(() => ctx.SaveChangesAsync());
        }
        finally
        {
            await ctx.Database.EnsureDeletedAsync();
        }
    }

    [Theory]
    [MemberData(nameof(SuccessCases))]
    public async Task Should_SumDecimal_When_ValueFitsPrecisionAndScale(int p, int s, decimal value)
    {
        const int multiplier = 5;
        await using var ctx = NewCtx(p, s);
        await ctx.Database.EnsureCreatedAsync();
        try
        {
            for (var i = 0; i < multiplier; i++)
                ctx.Add(new ParamItem { Price = value });
            await ctx.SaveChangesAsync();
            var got = await ctx.Items.Select(x => x.Price).SumAsync();

            Assert.Equal(value * multiplier, got);

            var tms = ctx.GetService<IRelationalTypeMappingSource>();
            var et = ctx.Model.FindEntityType(typeof(ParamItem))!;
            var prop = et.FindProperty(nameof(ParamItem.Price))!;
            var mapping = tms.FindMapping(prop)!;
            Assert.Equal($"Decimal({p}, {s})", mapping.StoreType);
        }
        finally
        {
            await ctx.Database.EnsureDeletedAsync();
        }
    }

    public sealed class ParametricDecimalContext(DbContextOptions<ParametricDecimalContext> options, int p, int s)
        : DbContext(options)
    {
        public DbSet<ParamItem> Items => Set<ParamItem>();

        protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.Entity<ParamItem>(b =>
        {
            b.ToTable($"Items_{p}_{s}_{Guid.NewGuid():N}");
            b.HasKey(x => x.Id);
            b.Property(x => x.Price).HasPrecision(p, s);
        });
    }

    public sealed class ParamItem
    {
        public int Id { get; init; }
        public decimal Price { get; init; }
    }
}
