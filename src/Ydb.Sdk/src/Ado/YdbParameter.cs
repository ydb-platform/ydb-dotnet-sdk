using System.Data;
using System.Data.Common;
using Ydb.Sdk.Value;
using static System.String;

namespace Ydb.Sdk.Ado;

public sealed class YdbParameter : DbParameter
{
    public YdbParameter(string parameterName)
    {
        ParameterName = parameterName;
    }

    public override void ResetDbType()
    {
        DbType = DbType.Object;
        IsNullable = false;
    }

    public override DbType DbType { get; set; } = DbType.Object;
    public override ParameterDirection Direction { get; set; } = ParameterDirection.InputOutput;
    public override bool IsNullable { get; set; }
    public override string ParameterName { get; set; }
    public override string SourceColumn { get; set; } = Empty;
    public override object? Value { get; set; }
    public override bool SourceColumnNullMapping { get; set; }
    public override int Size { get; set; }

    internal YdbValue YdbValue
    {
        get
        {
            if (Value is YdbValue value)
            {
                return value;
            }

            return DbType switch
            {
                DbType.String or DbType.AnsiString or DbType.AnsiStringFixedLength or DbType.StringFixedLength =>
                    Cast<string>(YdbValue.MakeOptionalUtf8, YdbValue.MakeUtf8),
                DbType.Binary => Cast<byte[]>(YdbValue.MakeOptionalString, YdbValue.MakeString),
                DbType.Byte => CastPrimitive<byte>(YdbValue.MakeOptionalUint8, YdbValue.MakeUint8),
                DbType.Boolean => CastPrimitive<bool>(YdbValue.MakeOptionalBool, YdbValue.MakeBool),
                DbType.Int32 => CastPrimitive<int>(YdbValue.MakeOptionalInt32, YdbValue.MakeInt32),
                DbType.Int64 => CastPrimitive<long>(YdbValue.MakeOptionalInt64, YdbValue.MakeInt64),
                DbType.UInt32 => CastPrimitive<uint>(YdbValue.MakeOptionalUint32, YdbValue.MakeUint32),
                DbType.UInt64 => CastPrimitive<ulong>(YdbValue.MakeOptionalUint64, YdbValue.MakeUint64),
                DbType.SByte => CastPrimitive<sbyte>(YdbValue.MakeOptionalInt8, YdbValue.MakeInt8),
                DbType.Int16 => CastPrimitive<short>(YdbValue.MakeOptionalInt16, YdbValue.MakeInt16),
                DbType.UInt16 => CastPrimitive<ushort>(YdbValue.MakeOptionalUint16, YdbValue.MakeUint16),
                DbType.Double => CastPrimitive<double>(YdbValue.MakeOptionalDouble, YdbValue.MakeDouble),
                DbType.Single => CastPrimitive<float>(YdbValue.MakeOptionalFloat, YdbValue.MakeFloat),
                DbType.Date => CastPrimitive<DateTime>(YdbValue.MakeOptionalDate, YdbValue.MakeDate),
                DbType.Time or DbType.DateTime => CastPrimitive<DateTime>(YdbValue.MakeOptionalDatetime,
                    YdbValue.MakeDatetime),
                DbType.DateTime2 => CastPrimitive<DateTime>(YdbValue.MakeOptionalTimestamp, YdbValue.MakeTimestamp),
                DbType.DateTimeOffset => CastPrimitive<DateTimeOffset>(
                    offset => YdbValue.MakeOptionalTimestamp(offset?.UtcDateTime),
                    offset => YdbValue.MakeTimestamp(offset.UtcDateTime)),
                DbType.Decimal or DbType.Currency => CastPrimitive<decimal>(YdbValue.MakeOptionalDecimal,
                    YdbValue.MakeDecimal),
                DbType.Object => YdbValueFromObject(),
                DbType.VarNumeric or DbType.Xml or DbType.Guid => throw new YdbAdoException(
                    $"Ydb don't supported this DbType: {DbType}"),
                _ => throw new ArgumentOutOfRangeException()
            };
        }
    }

    private YdbValue YdbValueFromObject()
    {
        DbType = Value switch
        {
            string => DbType.String,
            int => DbType.Int32,
            uint => DbType.UInt32,
            long => DbType.Int64,
            ulong => DbType.UInt64,
            bool => DbType.Boolean,
            byte => DbType.Byte,
            sbyte => DbType.SByte,
            float => DbType.Single,
            double => DbType.Double,
            short => DbType.Int16,
            ushort => DbType.UInt16,
            decimal => DbType.Decimal,
            byte[] => DbType.Binary,
            Guid => DbType.Guid,
            DateTime => DbType.DateTime,
            DateTimeOffset => DbType.DateTimeOffset,
            _ => throw new YdbAdoException($"Error converting {Value?.GetType().ToString() ?? "null"} to YdbValue")
        };
        IsNullable = false;

        return YdbValue;
    }

    private YdbValue Cast<T>(Func<T?, YdbValue> nullableValue, Func<T, YdbValue> notNullValue) where T : class
    {
        if (Value == null || IsNullable)
        {
            return nullableValue((T?)Value);
        }

        if (Value is T v)
        {
            return notNullValue(v);
        }

        throw new YdbAdoException($"Invalidate parameter state: expected value with type " +
                                  $"{typeof(T) + (IsNullable ? "?" : ", isNullable = false")}");
    }

    private YdbValue CastPrimitive<T>(Func<T?, YdbValue> nullableValue, Func<T, YdbValue> notNullValue) where T : struct
    {
        if (Value == null || IsNullable)
        {
            return nullableValue((T?)Value);
        }

        if (Value is T v)
        {
            return notNullValue(v);
        }

        throw new YdbAdoException($"Invalidate parameter state: expected value with type " +
                                  $"{typeof(T) + (IsNullable ? "?" : ", isNullable = false")}");
    }
}
