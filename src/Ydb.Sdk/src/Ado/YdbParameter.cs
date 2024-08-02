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
                DbType.Object when Value is not null => PrepareThenReturnYdbValue(),
                DbType.String or DbType.AnsiString or DbType.AnsiStringFixedLength or DbType.StringFixedLength when
                    Value is string valueString => YdbValue.MakeUtf8(valueString),
                DbType.Int32 when Value is int or sbyte or byte or short or ushort =>
                    YdbValue.MakeInt32(Convert.ToInt32(Value)),
                DbType.Int64 when Value is long or sbyte or byte or short or ushort or int or uint =>
                    YdbValue.MakeInt64(Convert.ToInt64(Value)),
                DbType.Boolean when Value is bool boolValue => YdbValue.MakeBool(boolValue),
                DbType.UInt32 when Value is uint or byte or ushort => YdbValue.MakeUint32(Convert.ToUInt32(Value)),
                DbType.UInt64
                    when Value is ulong or byte or ushort or uint => YdbValue.MakeUint64(Convert.ToUInt64(Value)),
                DbType.SByte when Value is sbyte sbyteValue => YdbValue.MakeInt8(sbyteValue),
                DbType.Int16 when Value is short or sbyte or byte => YdbValue.MakeInt16(Convert.ToInt16(Value)),
                DbType.UInt16 when Value is ushort or byte => YdbValue.MakeUint16(Convert.ToUInt16(Value)),
                DbType.Double when Value is double or float => YdbValue.MakeDouble(Convert.ToDouble(Value)),
                DbType.Single when Value is float floatValue => YdbValue.MakeFloat(floatValue),
                DbType.Date when Value is DateTime dateTimeValue => YdbValue.MakeDate(dateTimeValue),
                DbType.Time or DbType.DateTime
                    when Value is DateTime dateTimeValue => YdbValue.MakeDatetime(dateTimeValue),
                DbType.DateTime2 when Value is DateTime dateTime => YdbValue.MakeTimestamp(dateTime),
                DbType.DateTimeOffset when Value is DateTimeOffset dateTimeOffset =>
                    YdbValue.MakeTimestamp(dateTimeOffset.UtcDateTime),
                DbType.Decimal or DbType.Currency
                    when Value is decimal decimalValue => YdbValue.MakeDecimal(decimalValue),
                DbType.Binary when Value is byte[] bytes => YdbValue.MakeString(bytes),
                DbType.Byte when Value is byte valueByte => YdbValue.MakeUint8(valueByte),
                DbType.VarNumeric or DbType.Xml or DbType.Guid => throw new YdbException(
                    $"Ydb don't supported this DbType: {DbType}"),
                _ when !Enum.IsDefined(typeof(DbType), DbType) =>
                    throw new ArgumentOutOfRangeException(nameof(DbType), DbType, null),
                _ when Value is null => YdbValue.Null,
                _ => ThrowInvalidCast()
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
