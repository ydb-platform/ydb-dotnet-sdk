namespace Ydb.Sdk.Ado;

/// <summary>
/// A virtual database timestamp ordered by unsigned plan step, then transaction identifier.
/// </summary>
/// <remarks>
/// Values from different databases must not be compared. This is database time, not wall-clock time.
/// </remarks>
public sealed class YdbVirtualTimestamp : IComparable<YdbVirtualTimestamp>
{
    internal YdbVirtualTimestamp(VirtualTimestamp value)
    {
        PlanStep = value.PlanStep;
        TxId = value.TxId;
    }

    /// <summary>
    /// Gets the unsigned plan step.
    /// </summary>
    public ulong PlanStep { get; }

    /// <summary>
    /// Gets the unsigned transaction identifier.
    /// </summary>
    public ulong TxId { get; }

    /// <summary>
    /// Compares timestamps by plan step, then transaction identifier.
    /// </summary>
    public int CompareTo(YdbVirtualTimestamp? other)
    {
        if (other is null)
        {
            return 1;
        }

        var planStepComparison = PlanStep.CompareTo(other.PlanStep);
        return planStepComparison != 0 ? planStepComparison : TxId.CompareTo(other.TxId);
    }
}
