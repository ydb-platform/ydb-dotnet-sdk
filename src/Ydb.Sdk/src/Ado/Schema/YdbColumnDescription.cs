using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.YdbType;
using Ydb.Sdk.Value;
using Ydb.Table;
using static Ydb.Sdk.Ado.Internal.YdbTypeExtensions;

namespace Ydb.Sdk.Ado.Schema;

/// <summary>
/// Describes a column in a YDB table.
/// </summary>
/// <remarks>
/// This class represents column metadata including name, data type, nullability, and optional column family.
/// It is used both for describing existing table columns and for defining columns when creating new tables.
/// </remarks>
public sealed class YdbColumnDescription
{
    /// <summary>
    /// Gets the name of the column.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the storage type of the column.
    /// </summary>
    public YdbColumnType StorageType { get; }

    internal YdbColumnDescription(ColumnMeta columnMeta) : this(columnMeta.Name, new YdbColumnType(columnMeta.Type))
    {
        IsNullable = columnMeta.Type.TypeCase == Type.TypeOneofCase.OptionalType;
        Family = columnMeta.Family;
        if (columnMeta.DefaultValueCase == ColumnMeta.DefaultValueOneofCase.FromLiteral)
            DefaultValue = columnMeta.FromLiteral.ToColumnDefaultValue();
        if (columnMeta.DefaultValueCase == ColumnMeta.DefaultValueOneofCase.FromSequence)
            SequenceDescription = new YdbSequenceDescription(columnMeta.FromSequence);
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="YdbColumnDescription"/> class with the specified name and column type.
    /// </summary>
    /// <param name="name">The name of the column.</param>
    /// <param name="ydbColumnType">The YDB column type.</param>
    public YdbColumnDescription(string name, YdbColumnType ydbColumnType)
    {
        Name = name;
        StorageType = ydbColumnType;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="YdbColumnDescription"/> class with the specified name and database type.
    /// </summary>
    /// <param name="name">The name of the column.</param>
    /// <param name="ydbDbType">The YDB database type.</param>
    public YdbColumnDescription(string name, YdbDbType ydbDbType) : this(name, new YdbColumnType(ydbDbType))
    {
    }

    /// <summary>
    /// Gets or sets a value indicating whether the column allows null values.
    /// </summary>
    /// <value><c>true</c> if the column is nullable; otherwise, <c>false</c>. Default is <c>true</c>.</value>
    public bool IsNullable { get; init; } = true;

    /// <summary>
    /// Gets or sets the column family name for this column.
    /// </summary>
    /// <value>The column family name, or <c>null</c> if no family is specified.</value>
    /// <remarks>
    /// Column families allow grouping columns for storage optimization.
    /// </remarks>
    public string? Family { get; init; }

    /// <summary>
    /// Gets or sets the literal column default value.
    /// May contain a primitive CLR value, <see cref="YdbParameter"/>, <see cref="YdbValue"/>,
    /// or <see cref="TypedValue"/>.
    /// </summary>
    public object? DefaultValue { get; init; }

    /// <summary>
    /// Gets or sets the sequence-backed default value metadata.
    /// </summary>
    public YdbSequenceDescription? SequenceDescription { get; init; }

    internal ColumnMeta ToProto()
    {
        var columnMeta = new ColumnMeta
        {
            Name = Name,
            Type = StorageType.ToProto(),
            NotNull = !IsNullable
        };

        if (IsNullable)
        {
            columnMeta.Type = columnMeta.Type.OptionalType();
        }

        if (Family != null)
        {
            columnMeta.Family = Family;
        }

        if (DefaultValue != null && SequenceDescription != null)
        {
            throw new InvalidOperationException("Column default cannot contain both literal and sequence values.");
        }

        if (SequenceDescription != null)
        {
            columnMeta.FromSequence = SequenceDescription.ToProto();
            return columnMeta;
        }

        switch (DefaultValue)
        {
            case null:
                break;
            case YdbParameter parameter:
                columnMeta.FromLiteral = parameter.TypedValue;
                break;
            case YdbValue ydbValue:
                columnMeta.FromLiteral = ydbValue.GetProto();
                break;
            case TypedValue typedValue:
                columnMeta.FromLiteral = typedValue;
                break;
            default:
                columnMeta.FromLiteral = new YdbParameter("$defaultValue", DefaultValue).TypedValue;
                break;
        }

        return columnMeta;
    }
}
