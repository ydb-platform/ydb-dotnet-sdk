using Google.Protobuf.WellKnownTypes;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.YdbType;

namespace Ydb.Sdk.Value;

/// <summary>
/// Struct-only builder for YDB <c>List&lt;Struct&lt;...&gt;&gt;</c>.
/// Works directly with protobuf:
/// - Each call to <see cref="AddRow(object?[])"/> converts values into protobuf cells (<see cref="Ydb.Value"/>) and stores a row immediately.
/// - The struct schema (<see cref="StructType"/>) is derived from column type hints or from the first non-null sample per column.
/// - If a column has at least one <c>null</c>, its type becomes <c>Optional&lt;T&gt;</c>; individual null cells are encoded via <see cref="NullValue.NullValue"/>.
/// </summary>
public sealed class YdbList
{
    private readonly string[] _columns;
    private readonly YdbDbType[]? _typeHints;

    private readonly List<Ydb.Value> _rows = new();

    private readonly Type?[] _observedBaseTypes;
    private readonly bool[] _sawNull;

    /// <summary>
    /// Create a struct-mode list with column names; types will be inferred from the
    /// first non-null value per column (columns with any nulls become Optional&lt;T&gt;).
    /// </summary>
    public static YdbList Struct(params string[] columns) => new(columns);

    /// <summary>
    /// Create a struct-mode list with column names and explicit YDB type hints
    /// (array length must match <paramref name="columns"/>). Columns with any nulls
    /// will be emitted as Optional&lt;hintedType&gt;.
    /// </summary>
    public static YdbList Struct(string[] columns, YdbDbType[] types) => new(columns, types);

    /// <summary>
    /// Construct a struct-mode list. If <paramref name="types"/> is null, schema is inferred from values.
    /// </summary>
    private YdbList(string[] columns, YdbDbType[]? types = null)
    {
        if (columns is null || columns.Length == 0)
            throw new ArgumentException("Columns must be non-empty.", nameof(columns));
        if (types is not null && types.Length != columns.Length)
            throw new ArgumentException("Length of 'types' must match length of 'columns'.", nameof(types));

        _columns = columns;
        _typeHints = types;
        _observedBaseTypes = new Type[_columns.Length];
        _sawNull = new bool[_columns.Length];
    }

    /// <summary>
    /// Add a single positional row. The number of values must match the number of columns.
    /// Values are converted to protobuf cells and the row is stored immediately.
    /// </summary>
    public YdbList AddRow(params object?[] values)
    {
        if (values.Length != _columns.Length)
            throw new ArgumentException($"Expected {_columns.Length} values, got {values.Length}.");

        var cells = new List<Ydb.Value>(_columns.Length);

        for (var i = 0; i < _columns.Length; i++)
        {
            var v = values[i];

            if (v is null || v == DBNull.Value)
            {
                _sawNull[i] = true;
                cells.Add(new Ydb.Value { NullFlagValue = NullValue.NullValue });
                continue;
            }

            var tv = v switch
            {
                YdbValue yv => yv.GetProto(),
                YdbParameter p => p.TypedValue,
                _ => new YdbParameter { Value = v }.TypedValue
            };

            var t = tv.Type;
            if (t.TypeCase == Type.TypeOneofCase.OptionalType && t.OptionalType is not null)
                t = t.OptionalType.Item;

            _observedBaseTypes[i] ??= t;
            cells.Add(tv.Value);
        }

        _rows.Add(new Ydb.Value { Items = { cells } });
        return this;
    }

    /// <summary>
    /// Convert to a YDB <see cref="TypedValue"/> shaped as <c>List&lt;Struct&lt;...&gt;&gt;</c>.
    /// Columns that observed <c>null</c> values are emitted as <c>Optional&lt;T&gt;</c>;
    /// individual <c>null</c> cells are encoded via <see cref="NullValue.NullValue"/>.
    /// </summary>
    internal TypedValue ToTypedValue()
    {
        if (_rows.Count == 0 && (_typeHints is null || _typeHints.All(t => t == YdbDbType.Unspecified)))
            throw new InvalidOperationException(
                "Cannot infer Struct schema from an empty list without explicit YdbDbType hints.");

        var n = _columns.Length;
        var memberTypes = new Type[n];

        for (var i = 0; i < n; i++)
        {
            Type? baseType;

            if (_typeHints is not null && _typeHints[i] != YdbDbType.Unspecified)
            {
                baseType = new YdbParameter { YdbDbType = _typeHints[i], Value = DBNull.Value }.TypedValue.Type;

                if (baseType.TypeCase == Type.TypeOneofCase.OptionalType && baseType.OptionalType is not null)
                    baseType = baseType.OptionalType.Item;
            }
            else
            {
                baseType = _observedBaseTypes[i];
                if (baseType is null)
                    throw new InvalidOperationException(
                        $"Column '{_columns[i]}' has only nulls and no explicit YdbDbType. Provide a type hint.");
            }

            memberTypes[i] = _sawNull[i] && baseType.TypeCase != Type.TypeOneofCase.OptionalType
                ? new Type { OptionalType = new OptionalType { Item = baseType } }
                : baseType;
        }

        var structType = new StructType
        {
            Members =
            {
                _columns.Select((name, idx) => new StructMember
                {
                    Name = name,
                    Type = memberTypes[idx]
                })
            }
        };

        return new TypedValue
        {
            Type = new Type { ListType = new ListType { Item = new Type { StructType = structType } } },
            Value = new Ydb.Value { Items = { _rows } }
        };
    }
}
