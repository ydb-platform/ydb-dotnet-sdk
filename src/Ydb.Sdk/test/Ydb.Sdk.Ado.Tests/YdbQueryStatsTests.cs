using Xunit;
using Ydb.TableStats;

namespace Ydb.Sdk.Ado.Tests;

public class YdbQueryStatsTests
{
    [Fact]
    public void Statistics_CopyAllFieldsWithoutRetainingMutableProtobuf()
    {
        var operation = new OperationStats { Rows = ulong.MaxValue, Bytes = 2 };
        var table = new TableAccessStats
        {
            Name = "/local/table", PartitionsCount = 3, Reads = operation,
            Updates = new OperationStats { Rows = 4, Bytes = 5 },
            Deletes = new OperationStats { Rows = 6, Bytes = 7 }
        };
        var phase = new QueryPhaseStats
        {
            DurationUs = 8, CpuTimeUs = 9, AffectedShards = 10, LiteralPhase = true,
            TableAccess = { table }
        };
        var compilation = new CompilationStats { FromCache = true, DurationUs = 11, CpuTimeUs = 12 };
        var proto = new QueryStats
        {
            QueryPlan = "{}", QueryAst = "()", TotalDurationUs = 13, TotalCpuTimeUs = 14,
            ProcessCpuTimeUs = 15, Compilation = compilation, QueryPhases = { phase }
        };
        var stats = new YdbQueryStats(proto);

        proto.TotalDurationUs = 100;
        compilation.DurationUs = 100;
        phase.CpuTimeUs = 100;
        table.Name = "/local/other";
        operation.Rows = 100;
        proto.QueryPhases.Clear();
        phase.TableAccess.Clear();

        Assert.Equal("{}", stats.QueryPlan);
        Assert.Equal("()", stats.QueryAst);
        Assert.Equal((ulong)13, stats.TotalDurationUs);
        Assert.Equal((ulong)14, stats.TotalCpuTimeUs);
        Assert.Equal((ulong)15, stats.ProcessCpuTimeUs);
        Assert.True(stats.CompilationStats.FromCache);
        Assert.Equal((ulong)11, stats.CompilationStats.DurationUs);
        Assert.Equal((ulong)12, stats.CompilationStats.CpuTimeUs);
        var actualPhase = Assert.Single(stats.Phases);
        Assert.Equal((ulong)8, actualPhase.DurationUs);
        Assert.Equal((ulong)9, actualPhase.CpuTimeUs);
        Assert.Equal((ulong)10, actualPhase.AffectedShards);
        Assert.True(actualPhase.LiteralPhase);
        var actualTable = Assert.Single(actualPhase.TableAccesses);
        Assert.Equal("/local/table", actualTable.TableName);
        Assert.Equal((ulong)3, actualTable.PartitionsCount);
        Assert.Equal(ulong.MaxValue, actualTable.Reads.Rows);
        Assert.Equal((ulong)2, actualTable.Reads.Bytes);
        Assert.Equal((ulong)4, actualTable.Updates.Rows);
        Assert.Equal((ulong)5, actualTable.Updates.Bytes);
        Assert.Equal((ulong)6, actualTable.Deletes.Rows);
        Assert.Equal((ulong)7, actualTable.Deletes.Bytes);
    }

    [Fact]
    public void Statistics_MissingNestedMessagesHaveDefaultValues()
    {
        var stats = new YdbQueryStats(new QueryStats
        {
            QueryPhases = { new QueryPhaseStats { TableAccess = { new TableAccessStats() } } }
        });

        Assert.False(stats.CompilationStats.FromCache);
        Assert.Equal((ulong)0, stats.CompilationStats.DurationUs);
        Assert.Equal((ulong)0, stats.CompilationStats.CpuTimeUs);
        var table = Assert.Single(Assert.Single(stats.Phases).TableAccesses);
        Assert.Equal((ulong)0, table.Reads.Rows);
        Assert.Equal((ulong)0, table.Reads.Bytes);
        Assert.Equal((ulong)0, table.Updates.Rows);
        Assert.Equal((ulong)0, table.Updates.Bytes);
        Assert.Equal((ulong)0, table.Deletes.Rows);
        Assert.Equal((ulong)0, table.Deletes.Bytes);
    }
}
