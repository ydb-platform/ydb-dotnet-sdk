using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Ydb.Sdk.Value;
using static System.String;

namespace Ydb.Sdk.Ado;

public sealed class YdbParameter : DbParameter
{
    private string _parameterName = Empty;

    public YdbParameter()
    {
    }

    public YdbParameter(string parameterName, object value)
    {
        ParameterName = parameterName;
        Value = value;
    }

    public YdbParameter(string parameterName, DbType dbType, object? value = null)
    {
        ParameterName = parameterName;
        DbType = dbType;
        Value = value;
    }

    public override void ResetDbType()
    {
        DbType = DbType.Object;
        IsNullable = false;
    }

    public override DbType DbType { get; set; } = DbType.Object;
    public override ParameterDirection Direction { get; set; } = ParameterDirection.InputOutput;
    public override bool IsNullable { get; set; }

    [AllowNull]
    [DefaultValue("")]
    public override string ParameterName
    {
        get => _parameterName;
        set => _parameterName = value ?? throw new YdbAdoException("ParameterName must not be null!");
    }

    [AllowNull] [DefaultValue("")] public override string SourceColumn { get; set; } = Empty;
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
                DbType.Object => PrepareThenReturnYdbValue(),
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
                DbType.VarNumeric or DbType.Xml or DbType.Guid => throw new YdbAdoException(
                    $"Ydb don't supported this DbType: {DbType}"),
                _ => throw new ArgumentOutOfRangeException()
            };
        }
    }

    private YdbValue PrepareThenReturnYdbValue()
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
        if (IsNullable)
        {
            return nullableValue(Value is DBNull ? null : (T?)Value);
        }

        if (Value is T v)
        {
            return notNullValue(v);
        }

        throw new YdbAdoException($"Invalidate parameter state: expected value with type " +
                                  $"{typeof(T) + (IsNullable ? "" : ", isNullable = false")}");
    }

    private YdbValue CastPrimitive<T>(Func<T?, YdbValue> nullableValue, Func<T, YdbValue> notNullValue) where T : struct
    {
        if (IsNullable)
        {
            return nullableValue((T?)Value);
        }

        if (Value is T v)
        {
            return notNullValue(v);
        }

        throw new YdbAdoException($"Invalidate parameter state: expected value with type " +
                                  $"{typeof(T) + (IsNullable ? "" : ", isNullable = false")}");
    }

    public override bool Equals(object? obj)
    {
        if (obj is YdbParameter other)
        {
            return _parameterName == other._parameterName && DbType == other.DbType && Direction == other.Direction &&
                   IsNullable == other.IsNullable && SourceColumn == other.SourceColumn && Equals(Value, other.Value) &&
                   SourceColumnNullMapping == other.SourceColumnNullMapping && Size == other.Size;
        }

        return false;
    }

    public override int GetHashCode()
    {
        return ParameterName.GetHashCode();
    }
}
