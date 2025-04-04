namespace Ydb.Sdk.Ado.Schema;

public class YdbTableStats
{
    public YdbTableStats(Table.TableStats tableStats)
    {
        CreationTime = tableStats.CreationTime?.ToDateTime();
        ModificationTime = tableStats.ModificationTime?.ToDateTime();
        RowsEstimate = tableStats.RowsEstimate;
    }

    public DateTime? CreationTime { get; }

    public DateTime? ModificationTime { get; }

    public ulong RowsEstimate { get; }
}
