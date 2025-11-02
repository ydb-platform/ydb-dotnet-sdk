using System.Collections;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Ydb.Sdk.Ado.Internal;
using Ydb.Sdk.Ado.YdbType;
using Ydb.Sdk.Value;
using static Ydb.Sdk.Ado.Internal.YdbTypedValueExtensions;
using static Ydb.Sdk.Ado.Internal.YdbTypeExtensions;
using static Ydb.Sdk.Ado.Internal.YdbValueExtensions;

namespace Ydb.Sdk.Ado;

/// <summary>
/// Represents a parameter to a <see cref="YdbCommand"/> and optionally its mapping to a DataSet column.
/// This class cannot be inherited.
/// </summary>
/// <remarks>
/// YdbParameter provides a way to pass parameters to YDB commands, supporting both standard ADO.NET DbType
/// and YDB-specific YdbDbType values. It handles type conversion and null value representation for YDB operations.
/// </remarks>
public sealed class YdbParameter : DbParameter
{
    private YdbPrimitiveTypeInfo? _ydbPrimitiveTypeInfo;
    private YdbDbType _ydbDbType = YdbDbType.Unspecified;
    private string _parameterName = string.Empty;

    /// <summary>
    /// Initializes a new instance of the <see cref="YdbParameter"/> class.
    /// </summary>
    public YdbParameter()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="YdbParameter"/> class with the specified parameter name and value.
    /// </summary>
    /// <param name="parameterName">The name of the parameter.</param>
    /// <param name="value">The value of the parameter.</param>
    public YdbParameter(string parameterName, object value)
    {
        ParameterName = parameterName;
        Value = value;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="YdbParameter"/> class with the specified parameter name,
    /// database type, and optional value.
    /// </summary>
    /// <param name="parameterName">The name of the parameter.</param>
    /// <param name="dbType">The <see cref="DbType"/> of the parameter.</param>
    /// <param name="value">The value of the parameter, or null if not specified.</param>
    public YdbParameter(string parameterName, DbType dbType, object? value = null)
    {
        ParameterName = parameterName;
        DbType = dbType;
        Value = value;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="YdbParameter"/> class with the specified parameter name,
    /// YDB database type, and optional value.
    /// </summary>
    /// <param name="parameterName">The name of the parameter.</param>
    /// <param name="ydbDbType">The <see cref="YdbDbType"/> of the parameter.</param>
    /// <param name="value">The value of the parameter, or null if not specified.</param>
    public YdbParameter(string parameterName, YdbDbType ydbDbType, object? value = null)
    {
        ParameterName = parameterName;
        YdbDbType = ydbDbType;
        Value = value;
    }

    /// <summary>
    /// Resets the DbType property to its original state.
    /// </summary>
    /// <remarks>
    /// This method resets the YdbDbType to Unspecified, DbType to Object, and IsNullable to false.
    /// </remarks>
    public override void ResetDbType()
    {
        YdbDbType = YdbDbType.Unspecified;
        DbType = DbType.Object;
        IsNullable = false;
    }

    /// <summary>
    /// Gets or sets the YDB database type of the parameter.
    /// </summary>
    /// <remarks>
    /// YdbDbType provides YDB-specific data types that may not have direct equivalents in standard DbType.
    /// When set, this property automatically updates the corresponding DbType value.
    /// </remarks>
    public YdbDbType YdbDbType
    {
        get => _ydbDbType;
        set
        {
            _ydbPrimitiveTypeInfo = value.PrimitiveTypeInfo();
            if (value == YdbDbType.List)
            {
                throw new ArgumentOutOfRangeException(nameof(value),
                    "Cannot set YdbDbType to just List. " +
                    "Use Binary-Or with the element type (e.g. Array of dates is YdbDbType.List | YdbDbType.Date)."
                );
            }

            _ydbDbType = value;
        }
    }

    private DbType _dbType = DbType.Object;

    /// <summary>
    /// Gets or sets the DbType of the parameter.
    /// </summary>
    /// <remarks>
    /// When setting the DbType, the corresponding YdbDbType is automatically updated.
    /// This ensures compatibility with standard ADO.NET while maintaining YDB-specific functionality.
    /// </remarks>
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

    /// <summary>
    /// Gets or sets a value indicating whether the parameter accepts null values.
    /// </summary>
    /// <remarks>
    /// When true, the parameter can accept null values. This affects how null values
    /// are handled during parameter binding and execution.
    /// </remarks>
    public override bool IsNullable { get; set; }

    /// <summary>
    /// Gets or sets the name of the parameter.
    /// </summary>
    /// <remarks>
    /// The parameter name is automatically formatted to use YDB's parameter syntax ($parameterName).
    /// If the name starts with @, it's converted to $ syntax. If it doesn't start with $, the $ prefix is added.
    /// </remarks>
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

    /// <summary>
    /// Gets or sets the value of the parameter.
    /// </summary>
    /// <remarks>
    /// The value can be any object that is compatible with the parameter's data type.
    /// Null values are handled according to the IsNullable property setting.
    /// </remarks>
    public override object? Value { get; set; }

    public override bool SourceColumnNullMapping { get; set; }

    public override int Size { get; set; }

    /// <summary>
    /// Gets or sets the number of digits used to represent the Value property.
    /// </summary>
    /// <remarks>
    /// This property is used for decimal data type to specify
    /// the total number of digits to the left and right of the decimal point.
    /// </remarks>
    public override byte Precision { get; set; }

    /// <summary>
    /// Gets or sets the number of decimal places to which Value is resolved.
    /// </summary>
    /// <remarks>
    /// This property is used for decimal data type to specify
    /// the number of digits to the right of the decimal point.
    /// </remarks>
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
                return _ydbPrimitiveTypeInfo?.NullValue ??
                       (_ydbDbType == YdbDbType.Decimal
                           ? DecimalNull(Precision, Scale)
                           : _ydbDbType.HasFlag(YdbDbType.List)
                               ? ListNull((~YdbDbType.List & _ydbDbType).PrimitiveTypeInfo()?.YdbType ??
                                          DecimalType(Precision, Scale) /* only decimal is possible */)
                               : throw new InvalidOperationException(
                                   "Writing value of 'null' is not supported without explicit mapping to the YdbDbType")
                       );
            }

            return _ydbDbType switch
            {
                _ when _ydbPrimitiveTypeInfo != null => new TypedValue
                {
                    Type = _ydbPrimitiveTypeInfo.YdbType,
                    Value = _ydbPrimitiveTypeInfo.Pack(value) ?? throw ValueTypeNotSupportedException
                },
                YdbDbType.Decimal when value is decimal decimalValue => PackDecimal(decimalValue),
                YdbDbType.Unspecified => PackObject(value),
                _ when YdbDbType.HasFlag(YdbDbType.List) && value is IList itemsValue =>
                    PackList(itemsValue, ~YdbDbType.List & _ydbDbType),
                _ => throw ValueTypeNotSupportedException
            };
        }
    }

    private TypedValue PackObject(object value) => value switch
    {
        bool boolValue => new TypedValue { Type = YdbPrimitiveTypeInfo.Bool.YdbType, Value = PackBool(boolValue) },
        sbyte sbyteValue => new TypedValue { Type = YdbPrimitiveTypeInfo.Int8.YdbType, Value = PackInt8(sbyteValue) },
        short shortValue => new TypedValue { Type = YdbPrimitiveTypeInfo.Int16.YdbType, Value = PackInt16(shortValue) },
        int intValue => new TypedValue { Type = YdbPrimitiveTypeInfo.Int32.YdbType, Value = PackInt32(intValue) },
        long longValue => new TypedValue { Type = YdbPrimitiveTypeInfo.Int64.YdbType, Value = PackInt64(longValue) },
        byte byteValue => new TypedValue { Type = YdbPrimitiveTypeInfo.Uint8.YdbType, Value = PackUint8(byteValue) },
        ushort ushortValue => new TypedValue
            { Type = YdbPrimitiveTypeInfo.Uint16.YdbType, Value = PackUint16(ushortValue) },
        uint uintValue => new TypedValue { Type = YdbPrimitiveTypeInfo.Uint32.YdbType, Value = PackUint32(uintValue) },
        ulong ulongValue => new TypedValue
            { Type = YdbPrimitiveTypeInfo.Uint64.YdbType, Value = PackUint64(ulongValue) },
        float floatValue => new TypedValue { Type = YdbPrimitiveTypeInfo.Float.YdbType, Value = PackFloat(floatValue) },
        double doubleValue => new TypedValue
            { Type = YdbPrimitiveTypeInfo.Double.YdbType, Value = PackDouble(doubleValue) },
        decimal decimalValue => PackDecimal(decimalValue),
        Guid guidValue => new TypedValue { Type = YdbPrimitiveTypeInfo.Uuid.YdbType, Value = PackUuid(guidValue) },
        DateTime dateTimeValue => new TypedValue
            { Type = YdbPrimitiveTypeInfo.Timestamp.YdbType, Value = PackTimestamp(dateTimeValue) },
        DateOnly dateOnlyValue => new TypedValue
            { Type = YdbPrimitiveTypeInfo.Date.YdbType, Value = PackDate(dateOnlyValue.ToDateTime(TimeOnly.MinValue)) },
        byte[] bytesValue when value.GetType().GetElementType() == typeof(byte) /* array covariance */ => new TypedValue
            { Type = YdbPrimitiveTypeInfo.Bytes.YdbType, Value = PackBytes(bytesValue) },
        string stringValue => new TypedValue
            { Type = YdbPrimitiveTypeInfo.Text.YdbType, Value = PackText(stringValue) },
        TimeSpan timeSpanValue => new TypedValue
            { Type = YdbPrimitiveTypeInfo.Interval.YdbType, Value = PackInterval(timeSpanValue) },
        MemoryStream memoryStream => new TypedValue
            { Type = YdbPrimitiveTypeInfo.Bytes.YdbType, Value = PackBytes(memoryStream.ToArray()) },
        IList itemsValue => PackList(itemsValue),
        _ => throw new InvalidOperationException(
            $"Writing value of '{value.GetType()}' is not supported without explicit mapping to the YdbDbType")
    };

    private TypedValue PackDecimal(decimal value) => new()
        { Type = DecimalType(Precision, Scale), Value = value.PackDecimal(Precision, Scale) };

    private TypedValue PackList(IList items, YdbDbType ydbDbType = YdbDbType.Unspecified)
    {
        var elementType = GetElementType(items) ?? throw ValueTypeNotSupportedException;
        var primitiveTypeInfo = ydbDbType.PrimitiveTypeInfo() ?? YdbPrimitiveTypeInfo.TryResolve(elementType);

        if (primitiveTypeInfo != null)
        {
            var value = new Ydb.Value();
            var isOptional = false;

            foreach (var item in items)
            {
                if (item == null)
                {
                    isOptional = true;
                    value.Items.Add(YdbValueNull);
                }
                else
                {
                    value.Items.Add(primitiveTypeInfo.Pack(item) ?? throw ValueTypeNotSupportedException);
                }
            }

            var type = primitiveTypeInfo.YdbType;
            if (isOptional)
            {
                type = type.OptionalType();
            }

            return new TypedValue { Type = type.ListType(), Value = value };
        }

        if (ydbDbType == YdbDbType.Decimal || elementType.IsAssignableFrom(typeof(decimal)))
        {
            var value = new Ydb.Value();
            var isOptional = false;

            foreach (var item in items)
            {
                if (item == null)
                {
                    isOptional = true;
                    value.Items.Add(YdbValueNull);
                }
                else
                {
                    value.Items.Add(item is decimal decimalValue
                        ? decimalValue.PackDecimal(Precision, Scale)
                        : throw ValueTypeNotSupportedException);
                }
            }

            var type = DecimalType(Precision, Scale);
            if (isOptional)
            {
                type = type.OptionalType();
            }

            return new TypedValue { Type = type.ListType(), Value = value };
        }

        return (from object? item in items select PackObject(item ?? throw ValueTypeNotSupportedException)).ToArray()
            .List();
    }

    private InvalidOperationException ValueTypeNotSupportedException =>
        new($"Writing value of '{Value!.GetType()}' is not supported" +
            $" for parameters having YdbDbType '{YdbDbType.ToYdbTypeName()}'");

    private static System.Type? GetElementType(IList value)
    {
        var typeValue = value.GetType();

        if (typeValue.IsArray)
        {
            return typeValue.GetElementType();
        }

        return typeValue.GetInterfaces()
            .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IList<>))?
            .GetGenericArguments()[0];
    }
}
