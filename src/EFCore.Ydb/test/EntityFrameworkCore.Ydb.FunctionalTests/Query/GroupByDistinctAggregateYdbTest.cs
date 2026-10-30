using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class GroupByDistinctAggregateYdbTest
{
    [Fact]
    public async Task Distinct_inputs_preserve_nulls_filters_and_Int64_keys()
    {
        await using var store = YdbTestStore.GetOrCreate(nameof(GroupByDistinctAggregateYdbTest));
        var options = new DbContextOptionsBuilder<TestContext>();
        store.AddProviderOptions(options);
        await using var context = new TestContext(options.Options);
        await store.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();
        try
        {
            Row[] seed =
            [
                new() { Id = 1, Group = "A", Value = 2, Nullable = 7, LongValue = 0 },
                new() { Id = 2, Group = "A", Value = 2, Nullable = 7, LongValue = 9007199254740992 },
                new() { Id = 3, Group = "A", Value = 6, LongValue = 9007199254740993 },
                new() { Id = 4, Group = "B", Value = 3, LongValue = 3 },
                new() { Id = 5, Group = "B", Value = 3, LongValue = 3 },
                new() { Id = 6, Value = -1, Nullable = 8, LongValue = -1 }
            ];
            context.Rows.AddRange(seed);
            await context.SaveChangesAsync();

            int[] limit = [7];
            int[] offset = [0];
            var query = context.Rows.GroupBy(r => r.Group).Select(g => new
            {
                g.Key,
                Count = g.Where(r => r.Id < limit[0]).Select(r => r.Nullable).Distinct().Count(),
                LongCount = g.Where(r => r.Id < limit[0]).Select(r => r.Nullable).Distinct().LongCount(),
                Average = g.Where(r => r.Id < limit[0]).Select(r => (int?)(r.Value + offset[0])).Distinct().Average(),
                Sum = g.Where(r => r.Id < limit[0]).Select(r => r.Value + offset[0]).Distinct().Sum(),
                Min = g.Where(r => r.Id < limit[0]).Select(r => (int?)(r.Value + offset[0])).Distinct().Min(),
                Max = g.Where(r => r.Id < limit[0]).Select(r => (int?)(r.Value + offset[0])).Distinct().Max(),
                LongAverage = g.Select(r => r.LongValue).Distinct().Average(),
                LongSum = g.Select(r => r.LongValue).Distinct().Sum()
            });

            foreach (var values in new[] { (7, 0), (3, 10), (1, 0), (7, 0) })
            {
                (limit[0], offset[0]) = values;
                var expected = seed.GroupBy(r => r.Group).Select(g => new
                {
                    g.Key,
                    Count = g.Where(r => r.Id < limit[0]).Select(r => r.Nullable).Distinct().Count(),
                    LongCount = g.Where(r => r.Id < limit[0]).Select(r => r.Nullable).Distinct().LongCount(),
                    Average = g.Where(r => r.Id < limit[0]).Select(r => (int?)(r.Value + offset[0])).Distinct()
                        .Average(),
                    Sum = g.Where(r => r.Id < limit[0]).Select(r => r.Value + offset[0]).Distinct().Sum(),
                    Min = g.Where(r => r.Id < limit[0]).Select(r => (int?)(r.Value + offset[0])).Distinct().Min(),
                    Max = g.Where(r => r.Id < limit[0]).Select(r => (int?)(r.Value + offset[0])).Distinct().Max(),
                    LongAverage = g.Select(r => r.LongValue).Distinct().Average(),
                    LongSum = g.Select(r => r.LongValue).Distinct().Sum()
                }).OrderBy(r => r.Key).ToArray();

                var rows = await query.ToArrayAsync();
                Assert.Equal(expected, rows.OrderBy(r => r.Key).ToArray());
                Assert.Equal(18014398509481985L, Assert.Single(rows, r => r.Key == "A").LongSum);
            }

            await context.Rows.ExecuteDeleteAsync();
            Assert.Empty(await query.ToArrayAsync());
        }
        finally
        {
            await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS `GroupByDistinctAggregateRows`;");
        }
    }

    private sealed class Row
    {
        public int Id { get; set; }
        public string? Group { get; set; }
        public int Value { get; set; }
        public int? Nullable { get; set; }
        public long LongValue { get; set; }
    }

    private sealed class TestContext(DbContextOptions<TestContext> options) : DbContext(options)
    {
        public DbSet<Row> Rows => Set<Row>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Row>().ToTable("GroupByDistinctAggregateRows");
            modelBuilder.Entity<Row>().Property(r => r.Id).ValueGeneratedNever();
        }
    }
}
