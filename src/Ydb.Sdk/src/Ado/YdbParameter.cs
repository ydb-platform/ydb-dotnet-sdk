using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Ydb.Sdk.Ado.YdbType;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado;

public sealed class YdbParameter : DbParameter
{
    private static readonly TypedValue NullDefaultDecimal = YdbTypedValueExtensions.NullDecimal(22, 9);

    private static readonly Dictionary<YdbDbType, TypedValue> YdbNullByDbType = new()
    {
        { YdbDbType.Text, Type.Types.PrimitiveTypeId.Utf8.Null() },
        { YdbDbType.Bytes, Type.Types.PrimitiveTypeId.String.Null() },
        { YdbDbType.Bool, Type.Types.PrimitiveTypeId.Bool.Null() },
        { YdbDbType.Int8, Type.Types.PrimitiveTypeId.Int8.Null() },
        { YdbDbType.Int16, Type.Types.PrimitiveTypeId.Int16.Null() },
        { YdbDbType.Int32, Type.Types.PrimitiveTypeId.Int32.Null() },
        { YdbDbType.Int64, Type.Types.PrimitiveTypeId.Int64.Null() },
        { YdbDbType.UInt8, Type.Types.PrimitiveTypeId.Uint8.Null() },
        { YdbDbType.UInt16, Type.Types.PrimitiveTypeId.Uint16.Null() },
        { YdbDbType.UInt32, Type.Types.PrimitiveTypeId.Uint32.Null() },
        { YdbDbType.UInt64, Type.Types.PrimitiveTypeId.Uint64.Null() },
        { YdbDbType.Date, Type.Types.PrimitiveTypeId.Date.Null() },
        { YdbDbType.DateTime, Type.Types.PrimitiveTypeId.Datetime.Null() },
        { YdbDbType.Timestamp, Type.Types.PrimitiveTypeId.Timestamp.Null() },
        { YdbDbType.Interval, Type.Types.PrimitiveTypeId.Interval.Null() },
        { YdbDbType.Float, Type.Types.PrimitiveTypeId.Float.Null() },
        { YdbDbType.Double, Type.Types.PrimitiveTypeId.Double.Null() },
        { YdbDbType.Uuid, Type.Types.PrimitiveTypeId.Uuid.Null() },
        { YdbDbType.Json, Type.Types.PrimitiveTypeId.Json.Null() },
        { YdbDbType.JsonDocument, Type.Types.PrimitiveTypeId.JsonDocument.Null() }
    };

    private string _parameterName = string.Empty;

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

    public YdbParameter(string parameterName, YdbDbType ydbDbType, object? value = null)
    {
        ParameterName = parameterName;
        YdbDbType = ydbDbType;
        Value = value;
    }

    public override void ResetDbType()
    {
        YdbDbType = YdbDbType.Unspecified;
        DbType = DbType.Object;
        IsNullable = false;
    }

    public YdbDbType YdbDbType { get; set; } = YdbDbType.Unspecified;

    private DbType _dbType = DbType.Object;

    public override DbType DbType
    {
        get => _dbType;
        set
        {
            YdbDbType = value.ToYdbDbType();
            _dbType = value;
        }
    }

    public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;
    public override DataRowVersion SourceVersion { get; set; } = DataRowVersion.Current;
    public override bool IsNullable { get; set; }

    [AllowNull]
    [DefaultValue("")]
    public override string ParameterName
    {
        get => _parameterName;
        set => _parameterName = value switch
        {
            null => string.Empty,
            _ when value.StartsWith("$") => value,
            _ when value.StartsWith("@") && value.Length > 1 => $"${value[1..]}",
            _ => $"${value}"
        };
    }

    private string _sourceColumn = string.Empty;

    [AllowNull]
    [DefaultValue("")]
    public override string SourceColumn
    {
        get => _sourceColumn;
        set => _sourceColumn = value ?? string.Empty;
    }

    public override object? Value { get; set; }
    public override bool SourceColumnNullMapping { get; set; }
    public override int Size { get; set; }

    public override byte Precision { get; set; }
    public override byte Scale { get; set; }

    internal TypedValue TypedValue
    {
        get
        {
            var value = Value;

            if (value is YdbValue ydbValue)
            {
                return ydbValue.GetProto();
            }

            if (value == null || value == DBNull.Value)
            {
                return NullTypedValue();
            }

            return YdbDbType switch
            {
                YdbDbType.Text when value is string stringValue => stringValue.Text(),
                YdbDbType.Bool when value is bool boolValue => boolValue.Bool(),
                YdbDbType.Int8 when value is sbyte sbyteValue => sbyteValue.Int8(),
                YdbDbType.Int16 => MakeInt16(value),
                YdbDbType.Int32 => MakeInt32(value),
                YdbDbType.Int64 => MakeInt64(value),
                YdbDbType.UInt8 when value is byte byteValue => byteValue.Uint8(),
                YdbDbType.UInt16 => MakeUint16(value),
                YdbDbType.UInt32 => MakeUint32(value),
                YdbDbType.UInt64 => MakeUint64(value),
                YdbDbType.Float when value is float floatValue => floatValue.Float(),
                YdbDbType.Double => MakeDouble(value),
                YdbDbType.Decimal when value is decimal decimalValue => Decimal(decimalValue),
                YdbDbType.Bytes => MakeBytes(value),
                YdbDbType.Json when value is string stringValue => stringValue.Json(),
                YdbDbType.JsonDocument when value is string stringValue => stringValue.JsonDocument(),
                YdbDbType.Uuid when value is Guid guidValue => guidValue.Uuid(),
                YdbDbType.Date => MakeDate(value),
                YdbDbType.DateTime when value is DateTime dateTimeValue => dateTimeValue.Datetime(),
                YdbDbType.Timestamp => MakeTimestamp(value),
                YdbDbType.Interval when value is TimeSpan timeSpanValue => timeSpanValue.Interval(),
                YdbDbType.Unspecified => Cast(value),
                _ => throw ValueTypeNotSupportedException
            };
        }
    }

    private TypedValue MakeInt16(object value) => value switch
    {
        short shortValue => shortValue.Int16(),
        sbyte sbyteValue => YdbTypedValueExtensions.Int16(sbyteValue),
        byte byteValue => YdbTypedValueExtensions.Int16(byteValue),
        _ => throw ValueTypeNotSupportedException
    };

    private TypedValue MakeInt32(object value) => value switch
    {
        int intValue => intValue.Int32(),
        sbyte sbyteValue => YdbTypedValueExtensions.Int32(sbyteValue),
        byte byteValue => YdbTypedValueExtensions.Int32(byteValue),
        short shortValue => YdbTypedValueExtensions.Int32(shortValue),
        ushort ushortValue => YdbTypedValueExtensions.Int32(ushortValue),
        _ => throw ValueTypeNotSupportedException
    };

    private TypedValue MakeInt64(object value) => value switch
    {
        long longValue => longValue.Int64(),
        sbyte sbyteValue => YdbTypedValueExtensions.Int64(sbyteValue),
        byte byteValue => YdbTypedValueExtensions.Int64(byteValue),
        short shortValue => YdbTypedValueExtensions.Int64(shortValue),
        ushort ushortValue => YdbTypedValueExtensions.Int64(ushortValue),
        int intValue => YdbTypedValueExtensions.Int64(intValue),
        uint uintValue => YdbTypedValueExtensions.Int64(uintValue),
        _ => throw ValueTypeNotSupportedException
    };

    private TypedValue MakeUint16(object value) => value switch
    {
        ushort shortValue => shortValue.Uint16(),
        byte byteValue => YdbTypedValueExtensions.Uint16(byteValue),
        _ => throw ValueTypeNotSupportedException
    };

    private TypedValue MakeUint32(object value) => value switch
    {
        uint intValue => intValue.Uint32(),
        byte byteValue => YdbTypedValueExtensions.Uint32(byteValue),
        ushort ushortValue => YdbTypedValueExtensions.Uint32(ushortValue),
        _ => throw ValueTypeNotSupportedException
    };

    private TypedValue MakeUint64(object value) => value switch
    {
        ulong longValue => longValue.Uint64(),
        byte byteValue => YdbTypedValueExtensions.Uint64(byteValue),
        ushort ushortValue => YdbTypedValueExtensions.Uint64(ushortValue),
        uint uintValue => YdbTypedValueExtensions.Uint64(uintValue),
        _ => throw ValueTypeNotSupportedException
    };

    private TypedValue MakeDouble(object value) => value switch
    {
        double doubleValue => doubleValue.Double(),
        float floatValue => YdbTypedValueExtensions.Double(floatValue),
        _ => throw ValueTypeNotSupportedException
    };

    private TypedValue MakeBytes(object value) => value switch
    {
        byte[] bytesValue => bytesValue.Bytes(),
        MemoryStream memoryStream => memoryStream.ToArray().Bytes(),
        _ => throw ValueTypeNotSupportedException
    };

    private TypedValue MakeDate(object value) => value switch
    {
        DateTime dateTimeValue => dateTimeValue.Date(),
        DateOnly dateOnlyValue => dateOnlyValue.ToDateTime(TimeOnly.MinValue).Date(),
        _ => throw ValueTypeNotSupportedException
    };

    private TypedValue MakeTimestamp(object value) => value switch
    {
        DateTime dateTimeValue => dateTimeValue.Timestamp(),
        DateTimeOffset dateTimeOffsetValue => dateTimeOffsetValue.UtcDateTime.Timestamp(),
        _ => throw ValueTypeNotSupportedException
    };

    private TypedValue Cast(object value) => value switch
    {
        string stringValue => stringValue.Text(),
        bool boolValue => boolValue.Bool(),
        sbyte sbyteValue => sbyteValue.Int8(),
        short shortValue => shortValue.Int16(),
        int intValue => intValue.Int32(),
        long longValue => longValue.Int64(),
        byte byteValue => byteValue.Uint8(),
        ushort ushortValue => ushortValue.Uint16(),
        uint uintValue => uintValue.Uint32(),
        ulong ulongValue => ulongValue.Uint64(),
        float floatValue => floatValue.Float(),
        double doubleValue => doubleValue.Double(),
        decimal decimalValue => Decimal(decimalValue),
        Guid guidValue => guidValue.Uuid(),
        DateTime dateTimeValue => dateTimeValue.Timestamp(),
        DateTimeOffset dateTimeOffset => dateTimeOffset.UtcDateTime.Timestamp(),
        DateOnly dateOnlyValue => dateOnlyValue.ToDateTime(TimeOnly.MinValue).Date(),
        byte[] bytesValue => bytesValue.Bytes(),
        TimeSpan timeSpanValue => timeSpanValue.Interval(),
        MemoryStream memoryStream => memoryStream.ToArray().Bytes(),
        _ => throw new InvalidOperationException(
            $"Writing value of '{value.GetType()}' is not supported without explicit mapping to the YdbDbType")
    };

    private TypedValue Decimal(decimal value)
    {
        var p = Precision == 0 && Scale == 0 ? 22 : Precision;
        var s = Precision == 0 && Scale == 0 ? 9  : Scale;

        return value.Decimal((byte)p, (byte)s);
    }

    private TypedValue NullTypedValue()
    {
        if (YdbNullByDbType.TryGetValue(YdbDbType, out var value))
        {
            return value;
        }

        if (YdbDbType == YdbDbType.Decimal)
        {
            return Precision == 0 && Scale == 0
                ? NullDefaultDecimal
                : YdbTypedValueExtensions.NullDecimal(Precision, Scale);
        }

        throw new InvalidOperationException(
            "Writing value of 'null' is not supported without explicit mapping to the YdbDbType"
        );
    }

    private InvalidOperationException ValueTypeNotSupportedException =>
        new($"Writing value of '{Value!.GetType()}' is not supported for parameters having YdbDbType '{YdbDbType}'");
}
