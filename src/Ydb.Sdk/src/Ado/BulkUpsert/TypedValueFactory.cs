using System.Reflection;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado.BulkUpsert;

internal static class TypedValueFactory
{
    public static TypedValue FromObjects<T>(IReadOnlyCollection<T> rows)
    {
        if (rows.Count == 0)
            throw new ArgumentException("Rows collection is empty.", nameof(rows));

        var props = typeof(T).GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Where(p => p.CanRead).ToArray();

        var structs = new List<YdbValue>(rows.Count);

        foreach (var row in rows)
        {
            var members = new Dictionary<string, YdbValue>(props.Length);
            foreach (var p in props)
                members[p.Name] = ToYdbValue(p.GetValue(row), p.PropertyType);

            structs.Add(YdbValue.MakeStruct(members));
        }

        var list = YdbValue.MakeList(structs);
        return list.GetProto();
    }

    private static YdbValue ToYdbValue(object? value, System.Type clr)
    {
        if (value is null) return MakeOptional(clr);

        if (clr == typeof(bool)) return (YdbValue)(bool)value;
        if (clr == typeof(sbyte)) return (YdbValue)(sbyte)value;
        if (clr == typeof(short)) return (YdbValue)(short)value;
        if (clr == typeof(int)) return (YdbValue)(int)value;
        if (clr == typeof(long)) return (YdbValue)(long)value;
        if (clr == typeof(byte)) return (YdbValue)(byte)value;
        if (clr == typeof(ushort)) return (YdbValue)(ushort)value;
        if (clr == typeof(uint)) return (YdbValue)(uint)value;
        if (clr == typeof(ulong)) return (YdbValue)(ulong)value;
        if (clr == typeof(float)) return (YdbValue)(float)value;
        if (clr == typeof(double)) return (YdbValue)(double)value;
        if (clr == typeof(decimal)) return (YdbValue)(decimal)value;
        if (clr == typeof(DateTime)) return YdbValue.MakeTimestamp((DateTime)value);
        if (clr == typeof(TimeSpan)) return (YdbValue)(TimeSpan)value;
        if (clr == typeof(Guid)) return YdbValue.MakeUuid((Guid)value);
        if (clr == typeof(string)) return YdbValue.MakeUtf8((string)value);
        if (clr == typeof(byte[])) return YdbValue.MakeString((byte[])value);

        throw new NotSupportedException($"Type '{clr.FullName}' is not supported.");
    }

    private static YdbValue MakeOptional(System.Type clr)
    {
        var t = Nullable.GetUnderlyingType(clr) ?? clr;

        if (t == typeof(bool)) return YdbValue.MakeOptionalBool();
        if (t == typeof(sbyte)) return YdbValue.MakeOptionalInt8();
        if (t == typeof(short)) return YdbValue.MakeOptionalInt16();
        if (t == typeof(int)) return YdbValue.MakeOptionalInt32();
        if (t == typeof(long)) return YdbValue.MakeOptionalInt64();
        if (t == typeof(byte)) return YdbValue.MakeOptionalUint8();
        if (t == typeof(ushort)) return YdbValue.MakeOptionalUint16();
        if (t == typeof(uint)) return YdbValue.MakeOptionalUint32();
        if (t == typeof(ulong)) return YdbValue.MakeOptionalUint64();
        if (t == typeof(float)) return YdbValue.MakeOptionalFloat();
        if (t == typeof(double)) return YdbValue.MakeOptionalDouble();
        if (t == typeof(decimal)) return YdbValue.MakeOptionalDecimal();
        if (t == typeof(DateTime)) return YdbValue.MakeOptionalTimestamp();
        if (t == typeof(TimeSpan)) return YdbValue.MakeOptionalInterval();
        if (t == typeof(Guid)) return YdbValue.MakeOptionalUuid();
        if (t == typeof(string)) return YdbValue.MakeOptionalUtf8();
        if (t == typeof(byte[])) return YdbValue.MakeOptionalString();

        throw new NotSupportedException($"Null for '{t.FullName}' is not supported.");
    }
}