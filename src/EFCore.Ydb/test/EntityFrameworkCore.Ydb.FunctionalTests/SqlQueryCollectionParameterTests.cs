using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public class SqlQueryCollectionParameterTests
{
    private static readonly DateTime SomeTimestamp = DateTime.Parse("2025-11-02T18:47:14.112353");

    public static IEnumerable<object[]> GetCollectionTestCases()
    {
        yield return [new List<bool> { false, true, false }];
        yield return [(bool[])[false, true, false]];
        yield return [new List<sbyte> { 1, 2, 3 }];
        yield return [new sbyte[] { 1, 2, 3 }];
        yield return [new List<short> { 1, 2, 3 }];
        yield return [new short[] { 1, 2, 3 }];
        yield return [new List<int> { 1, 2, 3 }];
        yield return [(int[])[1, 2, 3]];
        yield return [new List<long> { 1, 2, 3 }];
        yield return [new long[] { 1, 2, 3 }];
        yield return [new List<byte> { 1, 2, 3 }];
        yield return [new List<ushort> { 1, 2, 3 }];
        yield return [new ushort[] { 1, 2, 3 }];
        yield return [new List<uint> { 1, 2, 3 }];
        yield return [new uint[] { 1, 2, 3 }];
        yield return [new List<ulong> { 1, 2, 3 }];
        yield return [new ulong[] { 1, 2, 3 }];
        yield return [new List<float> { 1, 2, 3 }];
        yield return [new float[] { 1, 2, 3 }];
        yield return [new List<double> { 1, 2, 3 }];
        yield return [new double[] { 1, 2, 3 }];
        yield return [new List<decimal> { 1, 2, 3 }];
        yield return [new decimal[] { 1, 2, 3 }];
        yield return [new List<string> { "1", "2", "3" }];
        yield return [(string[])["1", "2", "3"]];
        yield return [new List<byte[]> { new byte[] { 1, 1 }, new byte[] { 2, 2 }, new byte[] { 3, 3 } }];
        yield return [(byte[][])[[1, 1], [2, 2], [3, 3]]];
        yield return
            [new List<DateTime> { SomeTimestamp.AddDays(1), SomeTimestamp.AddDays(2), SomeTimestamp.AddDays(3) }];
        yield return [(DateTime[])[SomeTimestamp.AddDays(1), SomeTimestamp.AddDays(2), SomeTimestamp.AddDays(3)]];
        yield return [new List<TimeSpan> { TimeSpan.FromDays(1), TimeSpan.FromDays(2), TimeSpan.FromDays(3) }];
        yield return [(TimeSpan[])[TimeSpan.FromDays(1), TimeSpan.FromDays(2), TimeSpan.FromDays(3)]];
        yield return [new List<bool?> { false, true, false, null }];
        yield return [(bool?[])[false, true, false, null]];
        yield return [new List<sbyte?> { 1, 2, 3, null }];
        yield return [new sbyte?[] { 1, 2, 3, null }];
        yield return [new List<short?> { 1, 2, 3, null }];
        yield return [new short?[] { 1, 2, 3, null }];
        yield return [new List<int?> { 1, 2, 3, null }];
        yield return [(int?[])[1, 2, 3, null]];
        yield return [new List<long?> { 1, 2, 3, null }];
        yield return [new long?[] { 1, 2, 3, null }];
        yield return [new List<byte?> { 1, 2, 3, null }];
        yield return [new List<ushort?> { 1, 2, 3, null }];
        yield return [new ushort?[] { 1, 2, 3, null }];
        yield return [new List<uint?> { 1, 2, 3, null }];
        yield return [new uint?[] { 1, 2, 3, null }];
        yield return [new List<ulong?> { 1, 2, 3, null }];
        yield return [new ulong?[] { 1, 2, 3, null }];
        yield return [new List<float?> { 1, 2, 3, null }];
        yield return [new float?[] { 1, 2, 3, null }];
        yield return [new List<double?> { 1, 2, 3, null }];
        yield return [new double?[] { 1, 2, 3, null }];
        yield return [new List<decimal?> { 1, 2, 3, null }];
        yield return [new decimal?[] { 1, 2, 3, null }];
        yield return [new List<string?> { "1", "2", "3", null }];
        yield return [(string?[])["1", "2", "3", null]];
        yield return [new List<byte[]?> { new byte[] { 1, 1 }, new byte[] { 2, 2 }, new byte[] { 3, 3 }, null }];
        yield return [(byte[]?[])[[1, 1], [2, 2], [3, 3], null]];
        yield return
        [
            new List<DateTime?> { SomeTimestamp.AddDays(1), SomeTimestamp.AddDays(2), SomeTimestamp.AddDays(3), null }
        ];
        yield return
            [(DateTime?[])[SomeTimestamp.AddDays(1), SomeTimestamp.AddDays(2), SomeTimestamp.AddDays(3), null]];
        yield return
            [new List<TimeSpan?> { TimeSpan.FromDays(1), TimeSpan.FromDays(2), TimeSpan.FromDays(3), null }];
        yield return [(TimeSpan?[])[TimeSpan.FromDays(1), TimeSpan.FromDays(2), TimeSpan.FromDays(3), null]];
    }

    [Theory]
    [MemberData(nameof(GetCollectionTestCases))]
    public async Task SqlQuery_DoesNotExpandCollectionParameter_InClause<T>(IEnumerable<T> listIds)
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create("SqlQueryCollectionParameterTests");
        await using var testDbContext = new TestDbContext<T>();
        await testStore.CleanAsync(testDbContext);
        await testDbContext.Database.EnsureCreatedAsync();

        testDbContext.Items.AddRange(listIds.Where(value => value != null)
            .Select(value => new TestEntity<T> { Id = Guid.NewGuid(), Value = value }));
        await testDbContext.SaveChangesAsync();

        var rows = await testDbContext.Database.SqlQuery<TestEntity<T>>(
            $"SELECT * FROM TestEntity WHERE Value IN {listIds}").ToListAsync();

        Assert.Equal(3, rows.Count);
    }

    public sealed class TestDbContext<TValue> : DbContext
    {
        public DbSet<TestEntity<TValue>> Items => Set<TestEntity<TValue>>();

        protected override void OnModelCreating(ModelBuilder modelBuilder) =>
            modelBuilder.Entity<TestEntity<TValue>>(b =>
            {
                b.ToTable("TestEntity");
                b.HasKey(x => x.Id);
            });

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) => optionsBuilder
            .UseYdb("Host=localhost;Port=2136")
            .EnableServiceProviderCaching(false);
    }

    public sealed class TestEntity<TValue>
    {
        public Guid Id { get; init; }
        public TValue Value { get; set; } = default!;
    }
}
