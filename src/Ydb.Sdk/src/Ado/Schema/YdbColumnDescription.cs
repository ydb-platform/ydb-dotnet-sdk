using System.Globalization;
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
        DefaultValueExpression = columnMeta.DefaultValueCase switch
        {
            ColumnMeta.DefaultValueOneofCase.FromLiteral => FormatLiteralValue(columnMeta.FromLiteral),
            ColumnMeta.DefaultValueOneofCase.FromSequence => columnMeta.FromSequence?.ToString(),
            _ => null
        };
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
    /// Gets or sets the column default expression as reported by YDB schema metadata.
    /// </summary>
    public string? DefaultValueExpression { get; init; }

    private static string? FormatLiteralValue(TypedValue? literal)
    {
        if (literal is null)
        {
            return null;
        }

        var ydbValue = new YdbValue(literal.Type, literal.Value);
        return FormatYdbValue(ydbValue) ?? literal.ToString();
    }

    private static string? FormatYdbValue(YdbValue value) =>
        value.TypeId switch
        {
            YdbTypeId.Bool => value.GetBool().ToString(),
            YdbTypeId.Int8 => value.GetInt8().ToString(CultureInfo.InvariantCulture),
            YdbTypeId.Uint8 => value.GetUint8().ToString(CultureInfo.InvariantCulture),
            YdbTypeId.Int16 => value.GetInt16().ToString(CultureInfo.InvariantCulture),
            YdbTypeId.Uint16 => value.GetUint16().ToString(CultureInfo.InvariantCulture),
            YdbTypeId.Int32 => value.GetInt32().ToString(CultureInfo.InvariantCulture),
            YdbTypeId.Uint32 => value.GetUint32().ToString(CultureInfo.InvariantCulture),
            YdbTypeId.Int64 => value.GetInt64().ToString(CultureInfo.InvariantCulture),
            YdbTypeId.Uint64 => value.GetUint64().ToString(CultureInfo.InvariantCulture),
            YdbTypeId.Float => value.GetFloat().ToString(CultureInfo.InvariantCulture),
            YdbTypeId.Double => value.GetDouble().ToString(CultureInfo.InvariantCulture),
            YdbTypeId.String => Convert.ToBase64String(value.GetString()),
            YdbTypeId.Utf8 => value.GetUtf8(),
            YdbTypeId.Json => value.GetJson(),
            YdbTypeId.JsonDocument => value.GetJsonDocument(),
            YdbTypeId.OptionalType => value.GetOptional() is { } optionalValue ? FormatYdbValue(optionalValue) : null,
            _ => value.ToString()
        };

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

        return columnMeta;
    }
}
