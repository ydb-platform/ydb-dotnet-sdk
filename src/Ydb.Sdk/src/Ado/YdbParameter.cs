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
            if (Value is YdbValue ydbValue)
            {
                return ydbValue;
            }

            return DbType switch
            {
                DbType.Object => PrepareThenReturnYdbValue(),
                DbType.String or DbType.AnsiString or DbType.AnsiStringFixedLength
                    or DbType.StringFixedLength => Value switch
                    {
                        string valueString => YdbValue.MakeUtf8(valueString), null => YdbValue.TextNull,
                        _ => ThrowInvalidCast()
                    },
                DbType.Int32 => Value switch
                {
                    int intValue => YdbValue.MakeInt32(intValue),
                    sbyte or byte or short or ushort => YdbValue.MakeInt32(Convert.ToInt32(Value)),
                    null => YdbValue.Int32Null,
                    _ => ThrowInvalidCast()
                },
                DbType.Int64 => Value switch
                {
                    long longValue => YdbValue.MakeInt64(longValue),
                    sbyte or byte or short or ushort or int or uint => YdbValue.MakeInt64(Convert.ToInt64(Value)),
                    null => YdbValue.Int64Null,
                    _ => ThrowInvalidCast()
                },
                DbType.Boolean => Value switch
                {
                    bool boolValue => YdbValue.MakeBool(boolValue), null => YdbValue.BoolNull,
                    _ => ThrowInvalidCast()
                },
                DbType.UInt32 => Value switch
                {
                    uint uintValue => YdbValue.MakeUint32(uintValue),
                    byte or ushort => YdbValue.MakeUint32(Convert.ToUInt32(Value)),
                    null => YdbValue.Uint32Null,
                    _ => ThrowInvalidCast()
                },
                DbType.UInt64 => Value switch
                {
                    ulong ulongValue => YdbValue.MakeUint64(ulongValue),
                    byte or ushort or uint => YdbValue.MakeUint64(Convert.ToUInt64(Value)),
                    null => YdbValue.Uint64Null,
                    _ => ThrowInvalidCast()
                },
                DbType.SByte => Value switch
                {
                    sbyte sbyteValue => YdbValue.MakeInt8(sbyteValue), null => YdbValue.Int8Null,
                    _ => ThrowInvalidCast()
                },
                DbType.Int16 => Value switch
                {
                    short shortValue => YdbValue.MakeInt16(shortValue),
                    sbyte or byte => YdbValue.MakeInt16(Convert.ToInt16(Value)),
                    null => YdbValue.Int16Null,
                    _ => ThrowInvalidCast()
                },
                DbType.UInt16 => Value switch
                {
                    ushort ushortValue => YdbValue.MakeUint16(ushortValue),
                    byte => YdbValue.MakeUint16(Convert.ToUInt16(Value)),
                    null => YdbValue.Uint16Null,
                    _ => ThrowInvalidCast()
                },
                DbType.Double => Value switch
                {
                    double doubleValue => YdbValue.MakeDouble(doubleValue),
                    float => YdbValue.MakeDouble(Convert.ToSingle(Value)),
                    null => YdbValue.DoubleNull,
                    _ => ThrowInvalidCast()
                },
                DbType.Single => Value switch
                {
                    float floatValue => YdbValue.MakeFloat(floatValue), null => YdbValue.FloatNull,
                    _ => ThrowInvalidCast()
                },
                DbType.Date => Value switch
                {
                    DateTime dateTimeValue => YdbValue.MakeDate(dateTimeValue), null => YdbValue.DateNull,
                    _ => ThrowInvalidCast()
                },
                DbType.Time or DbType.DateTime => Value switch
                {
                    DateTime dateTimeValue => YdbValue.MakeDatetime(dateTimeValue), null => YdbValue.DatetimeNull,
                    _ => ThrowInvalidCast()
                },
                DbType.DateTime2 or DbType.DateTimeOffset => Value switch
                {
                    DateTime dateTimeValue => YdbValue.MakeTimestamp(dateTimeValue),
                    DateTimeOffset dateTimeOffset => YdbValue.MakeTimestamp(dateTimeOffset.UtcDateTime),
                    null => YdbValue.TimestampNull,
                    _ => ThrowInvalidCast()
                },
                DbType.Decimal or DbType.Currency => Value switch
                {
                    decimal decimalValue => YdbValue.MakeDecimal(decimalValue), null => YdbValue.DecimalNull,
                    _ => ThrowInvalidCast()
                },
                DbType.Binary => Value switch
                {
                    byte[] bytes => YdbValue.MakeString(bytes), null => YdbValue.BytesNull,
                    _ => ThrowInvalidCast()
                },
                DbType.Byte => Value switch
                {
                    byte valueByte => YdbValue.MakeUint8(valueByte), null => YdbValue.Uint8Null,
                    _ => ThrowInvalidCast()
                },
                DbType.VarNumeric or DbType.Xml or DbType.Guid => throw new YdbException(
                    $"Ydb don't supported this DbType: {DbType}"),
                _ => throw new ArgumentOutOfRangeException()
            };
        }
    }

    private YdbValue ThrowInvalidCast()
    {
        throw new InvalidCastException(
            $"Writing values of '{Value?.GetType()}' is not supported for parameters having DbType '{DbType}'");
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
}
