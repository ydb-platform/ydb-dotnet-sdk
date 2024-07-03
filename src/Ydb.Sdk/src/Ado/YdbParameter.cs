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
        set => _parameterName = value ?? throw new YdbException("ParameterName must not be null!");
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
                DbType.Byte => CastPrimitive(YdbValue.MakeOptionalUint8, YdbValue.MakeUint8, Convert.ToByte),
                DbType.Boolean => CastPrimitive(YdbValue.MakeOptionalBool, YdbValue.MakeBool, Convert.ToBoolean),
                DbType.Int32 => CastPrimitive(YdbValue.MakeOptionalInt32, YdbValue.MakeInt32, Convert.ToInt32),
                DbType.Int64 => CastPrimitive(YdbValue.MakeOptionalInt64, YdbValue.MakeInt64, Convert.ToInt64),
                DbType.UInt32 => CastPrimitive(YdbValue.MakeOptionalUint32, YdbValue.MakeUint32, Convert.ToUInt32),
                DbType.UInt64 => CastPrimitive(YdbValue.MakeOptionalUint64, YdbValue.MakeUint64, Convert.ToUInt64),
                DbType.SByte => CastPrimitive(YdbValue.MakeOptionalInt8, YdbValue.MakeInt8, Convert.ToSByte),
                DbType.Int16 => CastPrimitive(YdbValue.MakeOptionalInt16, YdbValue.MakeInt16, Convert.ToInt16),
                DbType.UInt16 => CastPrimitive(YdbValue.MakeOptionalUint16, YdbValue.MakeUint16, Convert.ToUInt16),
                DbType.Double => CastPrimitive(YdbValue.MakeOptionalDouble, YdbValue.MakeDouble, Convert.ToDouble),
                DbType.Single => CastPrimitive(YdbValue.MakeOptionalFloat, YdbValue.MakeFloat, Convert.ToSingle),
                DbType.Date => CastPrimitive(YdbValue.MakeOptionalDate, YdbValue.MakeDate, Convert.ToDateTime),
                DbType.Time or DbType.DateTime => CastPrimitive(YdbValue.MakeOptionalDatetime, YdbValue.MakeDatetime,
                    Convert.ToDateTime),
                DbType.DateTime2 => CastPrimitive(YdbValue.MakeOptionalTimestamp, YdbValue.MakeTimestamp,
                    Convert.ToDateTime),
                DbType.DateTimeOffset => CastPrimitive(offset => YdbValue.MakeOptionalTimestamp(offset?.UtcDateTime),
                    offset => YdbValue.MakeTimestamp(offset.UtcDateTime), o => (DateTimeOffset)o),
                DbType.Decimal or DbType.Currency => CastPrimitive(YdbValue.MakeOptionalDecimal, YdbValue.MakeDecimal,
                    Convert.ToDecimal),
                DbType.VarNumeric or DbType.Xml or DbType.Guid => throw new YdbException(
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
            _ => throw new YdbException($"Error converting {Value?.GetType().ToString() ?? "null"} to YdbValue")
        };
        IsNullable = false;

        return YdbValue;
    }

    private YdbValue Cast<T>(Func<T?, YdbValue> makeOptionalYdbValue, Func<T, YdbValue> makeYdbValue) where T : class
    {
        return Value switch
        {
            T value => makeYdbValue(value), // YdbValue will set to optional YdbValue type - it's ok
            DBNull or null when IsNullable => makeOptionalYdbValue(null),
            _ => throw new InvalidCastException($"Expected value with type {typeof(T)}, but actual {Value?.GetType()}")
        };
    }

    private YdbValue CastPrimitive<T>(Func<T?, YdbValue> makeOptionalYdbValue, Func<T, YdbValue> makeYdbValue,
        Func<object, T> converter) where T : struct
    {
        return Value switch
        {
            T value => makeYdbValue(value), // YdbValue will set to optional YdbValue type - it's ok
            DBNull or null when IsNullable => makeOptionalYdbValue(null),
            IConvertible convertible => makeYdbValue(converter.Invoke(convertible)),
            _ => throw new InvalidCastException($"Expected value with type {typeof(T)}, but actual {Value?.GetType()}")
        };
    }
}
