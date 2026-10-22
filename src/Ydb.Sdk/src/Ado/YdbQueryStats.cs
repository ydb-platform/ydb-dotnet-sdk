using Ydb.TableStats;

namespace Ydb.Sdk.Ado;

/// <summary>
/// Server query statistics, independent of the protobuf transport types.
/// </summary>
public sealed class YdbQueryStats
{
    internal YdbQueryStats(QueryStats value)
    {
        Phases = value.QueryPhases.Select(phase => new QueryPhase(phase)).ToList().AsReadOnly();
        CompilationStats = new Compilation(value.Compilation ?? new CompilationStats());
        QueryPlan = value.QueryPlan;
        QueryAst = value.QueryAst;
        ProcessCpuTimeUs = value.ProcessCpuTimeUs;
        TotalDurationUs = value.TotalDurationUs;
        TotalCpuTimeUs = value.TotalCpuTimeUs;
    }

    /// <summary>
    /// Gets the execution phases.
    /// </summary>
    public IReadOnlyList<QueryPhase> Phases { get; }

    /// <summary>
    /// Gets the compilation statistics.
    /// </summary>
    public Compilation CompilationStats { get; }

    /// <summary>
    /// Gets the query plan.
    /// </summary>
    public string QueryPlan { get; }

    /// <summary>
    /// Gets the query abstract syntax tree.
    /// </summary>
    public string QueryAst { get; }

    /// <summary>
    /// Gets the processing CPU time in microseconds.
    /// </summary>
    public ulong ProcessCpuTimeUs { get; }

    /// <summary>
    /// Gets the total duration in microseconds.
    /// </summary>
    public ulong TotalDurationUs { get; }

    /// <summary>
    /// Gets the total CPU time in microseconds.
    /// </summary>
    public ulong TotalCpuTimeUs { get; }

    /// <summary>
    /// Gets the number of logical base-table rows affected by DML, when collected by the server.
    /// </summary>
    public ulong? AffectedRows
    {
        get
        {
            var found = false;
            ulong total = 0;
            foreach (var phase in Phases)
            foreach (var table in phase.TableAccesses)
            {
                if (table.AffectedRows is not { } rows)
                    continue;

                found = true;
                total = checked(total + rows);
            }

            return found ? total : null;
        }
    }

    /// <summary>
    /// Statistics for query compilation.
    /// </summary>
    public sealed class Compilation
    {
        internal Compilation(CompilationStats value)
        {
            FromCache = value.FromCache;
            DurationUs = value.DurationUs;
            CpuTimeUs = value.CpuTimeUs;
        }

        /// <summary>
        /// Gets whether the compiled query came from the cache.
        /// </summary>
        public bool FromCache { get; }

        /// <summary>
        /// Gets the compilation duration in microseconds.
        /// </summary>
        public ulong DurationUs { get; }

        /// <summary>
        /// Gets the compilation CPU time in microseconds.
        /// </summary>
        public ulong CpuTimeUs { get; }
    }

    /// <summary>
    /// Statistics for one execution phase.
    /// </summary>
    public sealed class QueryPhase
    {
        internal QueryPhase(QueryPhaseStats value)
        {
            TableAccesses = value.TableAccess.Select(table => new TableAccess(table)).ToList().AsReadOnly();
            DurationUs = value.DurationUs;
            CpuTimeUs = value.CpuTimeUs;
            AffectedShards = value.AffectedShards;
            LiteralPhase = value.LiteralPhase;
        }

        /// <summary>
        /// Gets the table accesses in this phase.
        /// </summary>
        public IReadOnlyList<TableAccess> TableAccesses { get; }

        /// <summary>
        /// Gets the phase duration in microseconds.
        /// </summary>
        public ulong DurationUs { get; }

        /// <summary>
        /// Gets the phase CPU time in microseconds.
        /// </summary>
        public ulong CpuTimeUs { get; }

        /// <summary>
        /// Gets the number of affected shards.
        /// </summary>
        public ulong AffectedShards { get; }

        /// <summary>
        /// Gets whether this is a literal execution phase.
        /// </summary>
        public bool LiteralPhase { get; }
    }

    /// <summary>
    /// Statistics for access to one table.
    /// </summary>
    public sealed class TableAccess
    {
        internal TableAccess(TableAccessStats value)
        {
            TableName = value.Name;
            PartitionsCount = value.PartitionsCount;
            AffectedRows = value.HasAffectedRows ? value.AffectedRows : null;
            Reads = new Operation(value.Reads ?? new OperationStats());
            Updates = new Operation(value.Updates ?? new OperationStats());
            Deletes = new Operation(value.Deletes ?? new OperationStats());
        }

        /// <summary>
        /// Gets the table name.
        /// </summary>
        public string TableName { get; }

        /// <summary>
        /// Gets the number of accessed partitions.
        /// </summary>
        public ulong PartitionsCount { get; }

        /// <summary>
        /// Gets the logical base-table rows affected in this table access, when collected.
        /// </summary>
        public ulong? AffectedRows { get; }

        /// <summary>
        /// Gets the read statistics.
        /// </summary>
        public Operation Reads { get; }

        /// <summary>
        /// Gets the update statistics.
        /// </summary>
        public Operation Updates { get; }

        /// <summary>
        /// Gets the delete statistics.
        /// </summary>
        public Operation Deletes { get; }
    }

    /// <summary>
    /// Statistics for a table operation.
    /// </summary>
    public sealed class Operation
    {
        internal Operation(OperationStats value)
        {
            Rows = value.Rows;
            Bytes = value.Bytes;
        }

        /// <summary>
        /// Gets the number of affected rows.
        /// </summary>
        public ulong Rows { get; }

        /// <summary>
        /// Gets the number of affected bytes.
        /// </summary>
        public ulong Bytes { get; }
    }
}
