using Ydb.Table;

namespace Ydb.Sdk.Ado.Schema;

/// <summary>
/// Describes YDB <c>SequenceDescription</c> used as a column default source.
/// </summary>
public sealed class YdbSequenceDescription
{
    /// <summary>
    /// Sequence name (required by YDB).
    /// </summary>
    public string Name { get; init; }

    /// <summary>
    /// Minimum sequence value; if omitted, server default is used.
    /// </summary>
    public long? MinValue { get; init; }

    /// <summary>
    /// Maximum sequence value; if omitted, server default is used.
    /// </summary>
    public long? MaxValue { get; init; }

    /// <summary>
    /// Start value; if omitted, server default is used.
    /// </summary>
    public long? StartValue { get; init; }

    /// <summary>
    /// Number of values cached by server; if omitted, server default is used.
    /// </summary>
    public ulong? Cache { get; init; }

    /// <summary>
    /// Increment step; if omitted, server default is used.
    /// </summary>
    public long? Increment { get; init; }

    /// <summary>
    /// Whether sequence cycles on overflow; if omitted, server default is used.
    /// </summary>
    public bool? Cycle { get; init; }

    /// <summary>
    /// Creates sequence description with the specified name.
    /// </summary>
    public YdbSequenceDescription(string name)
    {
        Name = name;
    }

    internal YdbSequenceDescription(SequenceDescription sequenceDescription)
    {
        Name = sequenceDescription.Name;
        MinValue = sequenceDescription.HasMinValue ? sequenceDescription.MinValue : null;
        MaxValue = sequenceDescription.HasMaxValue ? sequenceDescription.MaxValue : null;
        StartValue = sequenceDescription.HasStartValue ? sequenceDescription.StartValue : null;
        Cache = sequenceDescription.HasCache ? sequenceDescription.Cache : null;
        Increment = sequenceDescription.HasIncrement ? sequenceDescription.Increment : null;
        Cycle = sequenceDescription.HasCycle ? sequenceDescription.Cycle : null;
    }

    internal SequenceDescription ToProto()
    {
        var sequence = new SequenceDescription { Name = Name };

        if (MinValue is { } minValue)
            sequence.MinValue = minValue;

        if (MaxValue is { } maxValue)
            sequence.MaxValue = maxValue;

        if (StartValue is { } startValue)
            sequence.StartValue = startValue;

        if (Cache is { } cache)
            sequence.Cache = cache;

        if (Increment is { } increment)
            sequence.Increment = increment;

        if (Cycle is { } cycle)
            sequence.Cycle = cycle;

        return sequence;
    }
}
