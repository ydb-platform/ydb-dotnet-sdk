using Google.Protobuf.WellKnownTypes;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.YdbType;

namespace Ydb.Sdk.Value;

/// <summary>
/// Struct-only builder for YDB <c>List&lt;Struct&lt;...&gt;&gt;</c>.
/// Define columns (optionally YDB types) and add positional rows; no external MakeStruct is needed.
/// </summary>
public sealed class YdbList
{
    private readonly string[] _columns;
    private readonly YdbDbType[]? _types;
    private readonly List<object?[]> _rows = new();

    /// <summary>Create Struct-mode list with column names; types will be inferred from the first non-null per column.</summary>
    public static YdbList Struct(params string[] columns) => new(columns);

    /// <summary>Create Struct-mode list with column names and explicit YDB types (same length as columns).</summary>
    public static YdbList Struct(string[] columns, YdbDbType[] types) => new(columns, types);

    /// <summary>Constructs Struct-mode list. If <paramref name="types"/> is null, types are inferred per column.</summary>
    public YdbList(string[] columns, YdbDbType[]? types = null)
    {
        if (columns is null || columns.Length == 0)
            throw new ArgumentException("Columns must be non-empty.", nameof(columns));
        if (types is not null && types.Length != columns.Length)
            throw new ArgumentException("Length of 'types' must match length of 'columns'.", nameof(types));

        _columns = columns;
        _types = types;
    }

    /// <summary>Add one positional row. Value count must match the number of columns.</summary>
    public YdbList AddRow(params object?[] values)
    {
        if (values.Length != _columns.Length)
            throw new ArgumentException($"Expected {_columns.Length} values, got {values.Length}.");
        _rows.Add(values);
        return this;
    }

    /// <summary>Convert to YDB <see cref="TypedValue"/> with shape <c>List&lt;Struct&lt;...&gt;&gt;</c>.</summary>
    internal TypedValue ToTypedValue()
    {
        if (_rows.Count == 0 && (_types is null || _types.All(t => t == YdbDbType.Unspecified)))
            throw new InvalidOperationException("Cannot infer Struct schema from an empty list without explicit YdbDbType hints.");

        var memberTypes = new Type[_columns.Length];
        for (int i = 0; i < _columns.Length; i++)
        {
            if (_types is not null && _types[i] != YdbDbType.Unspecified)
            {
                memberTypes[i] = new YdbParameter { YdbDbType = _types[i] }.TypedValue.Type;
                continue;
            }

            object? sample = null;
            foreach (var r in _rows)
            {
                var v = r[i];
                if (v is not null && v != DBNull.Value) { sample = v; break; }
            }
            if (sample is null)
                throw new InvalidOperationException(
                    $"Column '{_columns[i]}' has only nulls and no explicit YdbDbType. Provide a type hint.");

            memberTypes[i] = new YdbParameter { Value = sample }.TypedValue.Type;
        }

        var structType = new StructType
        {
            Members = { _columns.Select((name, idx) => new StructMember { Name = name, Type = memberTypes[idx] }) }
        };

        var ydbRows = new List<Ydb.Value>(_rows.Count);
        foreach (var r in _rows)
        {
            var fields = new List<Ydb.Value>(_columns.Length);
            for (int i = 0; i < _columns.Length; i++)
            {
                var v = r[i];
                if (v is null || v == DBNull.Value)
                {
                    fields.Add(new Ydb.Value { NullFlagValue = NullValue.NullValue });
                    continue;
                }

                var tv = v switch
                {
                    YdbValue yv    => yv.GetProto(),
                    YdbParameter p => p.TypedValue,
                    _              => new YdbParameter { Value = v }.TypedValue
                };
                fields.Add(tv.Value);
            }
            ydbRows.Add(new Ydb.Value { Items = { fields } });
        }

        return new TypedValue
        {
            Type  = new Type { ListType = new ListType { Item = new Type { StructType = structType } } },
            Value = new Ydb.Value { Items = { ydbRows } }
        };
    }
}
