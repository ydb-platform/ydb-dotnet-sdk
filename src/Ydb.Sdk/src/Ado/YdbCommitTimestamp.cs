namespace Ydb.Sdk.Ado;

/// <summary>
/// A commit timestamp returned by a StrictSerializableRW transaction.
/// </summary>
/// <remarks>
/// Timestamps can be compared only when they were obtained during the same opening of a
/// <see cref="YdbConnection"/>. The SDK cannot verify that separate connections point to
/// the same physical database.
/// </remarks>
public sealed class YdbCommitTimestamp : IComparable<YdbCommitTimestamp>
{
    private readonly object _connectionScope;

    internal YdbCommitTimestamp(VirtualTimestamp value, object connectionScope)
    {
        PlanStep = value.PlanStep;
        TxId = value.TxId;
        _connectionScope = connectionScope;
    }

    /// <summary>Gets the unsigned plan step.</summary>
    public ulong PlanStep { get; }

    /// <summary>Gets the unsigned transaction identifier.</summary>
    public ulong TxId { get; }

    /// <summary>Gets a copy of the YDB protobuf timestamp.</summary>
    public VirtualTimestamp Value => new() { PlanStep = PlanStep, TxId = TxId };

    /// <summary>Compares timestamps by plan step, then transaction identifier.</summary>
    /// <exception cref="InvalidOperationException">
    /// The timestamps were obtained from different connection openings.
    /// </exception>
    public int CompareTo(YdbCommitTimestamp? other)
    {
        if (other is null)
        {
            return 1;
        }

        if (!ReferenceEquals(_connectionScope, other._connectionScope))
        {
            throw new InvalidOperationException("Commit timestamps from different connection openings cannot be compared.");
        }

        var planStepComparison = PlanStep.CompareTo(other.PlanStep);
        return planStepComparison != 0 ? planStepComparison : TxId.CompareTo(other.TxId);
    }
}
