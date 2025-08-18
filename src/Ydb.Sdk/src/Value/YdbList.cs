using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.YdbType;

namespace Ydb.Sdk.Value;

/// <summary>
/// Universal wrapper for YDB lists.
/// <para>
/// - Plain mode (back-compat): wraps any <see cref="IEnumerable{object}"/> and produces <c>List&lt;T&gt;</c>.
/// </para>
/// <para>
/// - Struct mode: builds <c>List&lt;Struct&lt;...&gt;&gt;</c> without using <c>YdbValue.MakeStruct</c> on the outside.
///   You define column names (and optional types) and push rows positionally.
/// </para>
/// </summary>
public sealed class YdbList
{
    // -------- Plain mode --------
    private readonly IReadOnlyList<object>? _items;

    // -------- Struct mode --------
    private readonly string[]? _columns;
    private readonly YdbDbType[]? _types;
    private readonly List<object?[]>? _rows;

    /// <summary>
    /// Plain mode constructor (kept for backward compatibility).
    /// Produces <c>List&lt;T&gt;</c> by inferring element types.
    /// </summary>
    public YdbList(IEnumerable<object> items)
    {
        _items = items as IReadOnlyList<object> ?? items.ToList();
    }

    /// <summary>
    /// Start Struct mode with column names (types will be inferred from the first non-null row).
    /// </summary>
    public static YdbList Struct(params string[] columns) => new(columns, null);

    /// <summary>
    /// Start Struct mode with column names and explicit YDB types (same length as <paramref name="columns"/>).
    /// Use explicit types if you plan to pass <c>null</c> values and want typed NULLs.
    /// </summary>
    public static YdbList Struct(string[] columns, YdbDbType[]? types) => new(columns, types);

    private YdbList(string[] columns, YdbDbType[]? types)
    {
        if (types is not null && types.Length != columns.Length)
            throw new ArgumentException("Length of 'types' must match length of 'columns'.", nameof(types));

        _columns = columns;
        _types = types;
        _rows = new List<object?[]>();
    }

    /// <summary>
    /// Add one positional row (Struct mode). Values must match the number of columns.
    /// </summary>
    public YdbList AddRow(params object?[] values)
    {
        EnsureStruct();
        if (values.Length != _columns!.Length)
            throw new ArgumentException($"Expected {_columns.Length} values, got {values.Length}.");
        _rows!.Add(values);
        return this;
    }

    /// <summary>
    /// Converts this wrapper to a YDB <see cref="TypedValue"/>.
    /// In plain mode returns <c>List&lt;T&gt;</c>; in struct mode returns <c>List&lt;Struct&lt;...&gt;&gt;</c>.
    /// </summary>
    internal TypedValue ToTypedValue()
        => _columns is null ? ToTypedValuePlain() : ToTypedValueStruct();

    // -------- Implementation: plain mode --------
    private TypedValue ToTypedValuePlain()
    {
        var typed = new List<TypedValue>(_items!.Count);
        foreach (var item in _items)
        {
            var tv = item switch
            {
                YdbValue yv => yv.GetProto(),
                YdbParameter p => p.TypedValue,
                _ => new YdbParameter { Value = item }.TypedValue
            };
            typed.Add(tv);
        }

        return typed.List();
    }

    // -------- Implementation: struct mode --------
    private TypedValue ToTypedValueStruct()
    {
        if (_rows!.Count == 0 && (_types is null || _types.All(t => t == YdbDbType.Unspecified)))
            throw new InvalidOperationException(
                "Cannot infer Struct schema from an empty list without explicit YdbDbType hints.");

        var memberTypes = new List<Type>(_columns!.Length);
        for (var i = 0; i < _columns.Length; i++)
        {
            if (_types is not null && _types[i] != YdbDbType.Unspecified)
            {
                var tv = new YdbParameter { YdbDbType = _types[i] }.TypedValue;
                memberTypes.Add(tv.Type);
                continue;
            }

            var sample = (from r in _rows where r[i] is not null and not DBNull select r[i]).FirstOrDefault();
            if (sample is null)
                throw new InvalidOperationException(
                    $"Column '{_columns[i]}' has only nulls and no explicit YdbDbType. Provide a type hint.");

            var inferred = new YdbParameter { Value = sample }.TypedValue;
            memberTypes.Add(inferred.Type);
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

        var ydbRows = new List<Ydb.Value>(_rows.Count);
        foreach (var r in _rows)
        {
            var fields = new List<Ydb.Value>(_columns.Length);
            for (var i = 0; i < _columns.Length; i++)
            {
                var v = r[i];

                if (_types is not null && _types[i] != YdbDbType.Unspecified)
                {
                    var tv = new YdbParameter { YdbDbType = _types[i], Value = v }.TypedValue;
                    fields.Add(tv.Value);
                }
                else
                {
                    if (v is null || v == DBNull.Value)
                        throw new InvalidOperationException(
                            $"Column '{_columns[i]}' has null value but no explicit YdbDbType. Provide a type hint.");

                    var tv = v switch
                    {
                        YdbValue yv   => yv.GetProto(),
                        YdbParameter p => p.TypedValue,
                        _             => new YdbParameter { Value = v }.TypedValue
                    };
                    fields.Add(tv.Value);
                }
            }
            ydbRows.Add(new Ydb.Value { Items = { fields } });
        }

        return new TypedValue
        {
            Type  = new Type { ListType = new ListType { Item = new Type { StructType = structType } } },
            Value = new Ydb.Value { Items = { ydbRows } }
        };
    }

    private void EnsureStruct()
    {
        if (_columns is null)
            throw new InvalidOperationException(
                "This YdbList was created in plain mode. Use YdbList.Struct(...) to build List<Struct<...>>.");
    }
}
