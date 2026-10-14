using Ydb.TableStats;

namespace Ydb.Sdk.Ado;

/// <summary>Contains metadata returned after a successful query execution.</summary>
public sealed class YdbQueryInfo
{
    internal YdbQueryInfo(QueryStats? queryStats)
    {
        Stats = queryStats is null ? null : new YdbQueryStats(queryStats);
    }

    /// <summary>Gets the query statistics, or null when the server did not return them.</summary>
    public YdbQueryStats? Stats { get; }
}
