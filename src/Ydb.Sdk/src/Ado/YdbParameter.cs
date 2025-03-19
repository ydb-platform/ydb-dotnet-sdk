using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado;

public sealed class YdbParameter : DbParameter
{
    private static readonly Dictionary<DbType, YdbValue> YdbNullByDbType = new()
    {
        { DbType.String, YdbValue.MakeOptionalUtf8() },
        { DbType.AnsiString, YdbValue.MakeOptionalUtf8() },
        { DbType.AnsiStringFixedLength, YdbValue.MakeOptionalUtf8() },
        { DbType.StringFixedLength, YdbValue.MakeOptionalUtf8() },
        { DbType.Int32, YdbValue.MakeOptionalInt32() },
        { DbType.Int64, YdbValue.MakeOptionalInt64() },
        { DbType.Boolean, YdbValue.MakeOptionalBool() },
        { DbType.UInt32, YdbValue.MakeOptionalUint32() },
        { DbType.UInt64, YdbValue.MakeOptionalUint64() },
        { DbType.SByte, YdbValue.MakeOptionalInt8() },
        { DbType.Int16, YdbValue.MakeOptionalInt16() },
        { DbType.UInt16, YdbValue.MakeOptionalUint16() },
        { DbType.Double, YdbValue.MakeOptionalDouble() },
        { DbType.Single, YdbValue.MakeOptionalFloat() },
        { DbType.Date, YdbValue.MakeOptionalDate() },
        { DbType.DateTime, YdbValue.MakeOptionalDatetime() },
        { DbType.Binary, YdbValue.MakeOptionalString() },
        { DbType.Byte, YdbValue.MakeOptionalUint8() },
        { DbType.DateTime2, YdbValue.MakeOptionalTimestamp() },
        { DbType.DateTimeOffset, YdbValue.MakeOptionalTimestamp() },
        { DbType.Decimal, YdbValue.MakeOptionalDecimal() },
        { DbType.Currency, YdbValue.MakeOptionalDecimal() },
        { DbType.Guid, YdbValue.MakeOptionalUuid() }
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

    public override void ResetDbType()
    {
        DbType = DbType.Object;
        IsNullable = false;
    }

    public override DbType DbType { get; set; } = DbType.Object;
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

    internal YdbValue YdbValue => Value switch
    {
        YdbValue ydbValue => ydbValue,
        null or DBNull when YdbNullByDbType.TryGetValue(DbType, out var value) => value,
        string valueString when DbType is DbType.String or DbType.AnsiString or DbType.AnsiStringFixedLength
            or DbType.StringFixedLength or DbType.Object => YdbValue.MakeUtf8(valueString),
        bool boolValue when DbType is DbType.Boolean or DbType.Object => YdbValue.MakeBool(boolValue),
        DateTime dateTimeValue => DbType switch
        {
            DbType.Date => YdbValue.MakeDate(dateTimeValue),
            DbType.DateTime => YdbValue.MakeDatetime(dateTimeValue),
            DbType.DateTime2 or DbType.Object => YdbValue.MakeTimestamp(dateTimeValue),
            _ => ThrowInvalidOperation()
        },
        DateTimeOffset dateTimeOffset when DbType is DbType.DateTimeOffset or DbType.Object =>
            YdbValue.MakeTimestamp(dateTimeOffset.UtcDateTime),
        float floatValue => DbType switch
        {
            DbType.Single or DbType.Object => YdbValue.MakeFloat(floatValue),
            DbType.Double => YdbValue.MakeDouble(floatValue),
            _ => ThrowInvalidOperation()
        },
        double doubleValue when DbType is DbType.Double or DbType.Object => YdbValue.MakeDouble(doubleValue),
        int intValue => DbType switch
        {
            DbType.Int32 or DbType.Object => YdbValue.MakeInt32(intValue),
            DbType.Int64 => YdbValue.MakeInt64(intValue),
            _ => ThrowInvalidOperation()
        },
        long longValue when DbType is DbType.Int64 or DbType.Object => YdbValue.MakeInt64(longValue),
        decimal decimalValue when DbType is DbType.Decimal or DbType.Currency or DbType.Object =>
            YdbValue.MakeDecimal(decimalValue),
        ulong ulongValue when DbType is DbType.UInt64 or DbType.Object => YdbValue.MakeUint64(ulongValue),
        uint uintValue => DbType switch
        {
            DbType.UInt32 or DbType.Object => YdbValue.MakeUint32(uintValue),
            DbType.UInt64 => YdbValue.MakeUint64(uintValue),
            DbType.Int64 => YdbValue.MakeInt64(uintValue),
            _ => ThrowInvalidOperation()
        },
        byte byteValue => DbType switch
        {
            DbType.Byte or DbType.Object => YdbValue.MakeUint8(byteValue),
            DbType.Int64 => YdbValue.MakeInt64(byteValue),
            DbType.Int32 => YdbValue.MakeInt32(byteValue),
            DbType.Int16 => YdbValue.MakeInt16(byteValue),
            DbType.UInt64 => YdbValue.MakeUint64(byteValue),
            DbType.UInt32 => YdbValue.MakeUint32(byteValue),
            DbType.UInt16 => YdbValue.MakeUint16(byteValue),
            _ => ThrowInvalidOperation()
        },
        sbyte sbyteValue => DbType switch
        {
            DbType.SByte or DbType.Object => YdbValue.MakeInt8(sbyteValue),
            DbType.Int64 => YdbValue.MakeInt64(sbyteValue),
            DbType.Int32 => YdbValue.MakeInt32(sbyteValue),
            DbType.Int16 => YdbValue.MakeInt16(sbyteValue),
            _ => ThrowInvalidOperation()
        },
        ushort ushortValue => DbType switch
        {
            DbType.UInt16 or DbType.Object => YdbValue.MakeUint16(ushortValue),
            DbType.Int64 => YdbValue.MakeInt64(ushortValue),
            DbType.Int32 => YdbValue.MakeInt32(ushortValue),
            DbType.UInt64 => YdbValue.MakeUint64(ushortValue),
            DbType.UInt32 => YdbValue.MakeUint32(ushortValue),
            _ => ThrowInvalidOperation()
        },
        short shortValue => DbType switch
        {
            DbType.Int16 or DbType.Object => YdbValue.MakeInt16(shortValue),
            DbType.Int64 => YdbValue.MakeInt64(shortValue),
            DbType.Int32 => YdbValue.MakeInt32(shortValue),
            _ => ThrowInvalidOperation()
        },
        byte[] bytesValue when DbType is DbType.Binary or DbType.Object => YdbValue.MakeString(bytesValue),
        Guid guidValue when DbType is DbType.Guid or DbType.Object => YdbValue.MakeUuid(guidValue),
        MemoryStream memoryStream when DbType is DbType.Binary or DbType.Object => YdbValue.MakeString(
            memoryStream.ToArray()),
        _ when DbType is DbType.VarNumeric or DbType.Xml or DbType.Time =>
            throw new YdbException($"Ydb don't supported this DbType: {DbType}"),
        _ => ThrowInvalidOperation()
    };

    private YdbValue ThrowInvalidOperation() => throw new InvalidOperationException(
        $"Writing value of '{Value?.GetType().ToString() ?? "null"}' is not supported for parameters having DbType '{DbType}'");
}
