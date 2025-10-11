using System.Data;

namespace Ydb.Sdk.Ado.YdbType;

/// <summary>
/// Specifies the data type of the <see cref="YdbParameter"/>.
/// </summary>
/// <remarks>
/// YdbDbType represents the primitive data types supported by YDB.
/// 
/// For more information about YDB primitive types, see:
/// <see href="https://ydb.tech/docs/en/yql/reference/types/primitive">YDB Primitive Types Documentation</see>
/// </remarks>
public enum YdbDbType
{
    /// <summary>
    /// Unspecified data type.
    /// </summary>
    /// <remarks>
    /// When this type is used, the <see cref="YdbParameter"/> tries to automatically determine
    /// the appropriate YDB data type based on the system type of the parameter value.
    /// This provides convenience, but it may not always match the intended YDB type and
    /// doesn't support null values.
    /// </remarks>
    Unspecified,

    /// <summary>
    /// Boolean value.
    /// </summary>
    Bool,

    /// <summary>
    /// A signed integer.
    /// Acceptable values: from -2^7 to 2^7 – 1.
    /// </summary>
    Int8,

    /// <summary>
    /// A signed integer.
    /// Acceptable values: from –2 ^ 15 to 2 ^ 15 – 1.
    /// </summary>
    Int16,

    /// <summary>
    /// A signed integer.
    /// Acceptable values: from –2 ^ 31 to 2 ^ 31 – 1.
    /// </summary>
    Int32,

    /// <summary>
    /// A signed integer.
    /// Acceptable values: from –2 ^ 63 to 2 ^ 63 – 1.
    /// </summary>
    Int64,

    /// <summary>
    /// An unsigned integer.
    /// Acceptable values: from 0 to 2 ^ 16 – 1.
    /// </summary>
    UInt8,

    /// <summary>
    /// An unsigned integer.
    /// Acceptable values: from 0 to 2 ^ 16 – 1.
    /// </summary>
    UInt16,

    /// <summary>
    /// An unsigned integer.
    /// Acceptable values: from 0 to 2 ^ 32 – 1.
    /// </summary>
    UInt32,

    /// <summary>
    /// An unsigned integer.
    /// Acceptable values: from 0 to 2 ^ 64 – 1.
    /// </summary>
    UInt64,

    /// <summary>
    /// A real number with variable precision, 4 bytes in size.
    /// </summary>
    /// <remarks>
    /// Can't be used in the primary key.
    /// </remarks>
    Float,

    /// <summary>
    /// A real number with variable precision, 8 bytes in size.
    /// </summary>
    /// <remarks>
    /// Can't be used in the primary key.
    /// </remarks>
    Double,

    /// <summary>
    /// Represents a fixed-point real number type with user-specified precision and scale, stored in 16 bytes.
    /// Precision is the maximum total number of decimal digits stored (1 to 35).
    /// Scale is the number of decimal digits to the right of the decimal point (0 to the precision value).
    /// </summary>
    /// <remarks>
    /// Precision and Scale must be specified in <see cref="Ydb.Sdk.Ado.YdbParameter"/>.
    /// If not specified, the default values (precision: 22, scale: 9) are used.
    /// </remarks>
    Decimal,

    /// <summary>
    /// Binary object (BLOB) is represented as bytes.
    /// </summary>
    /// <remarks>
    /// This type is an alias for the deprecated <c>String</c> type.
    /// </remarks>
    Bytes,

    /// <summary>
    /// Text encoded in UTF-8.
    /// </summary>
    /// <remarks>
    /// This type is an alias for the deprecated <c>Utf8</c> type.
    /// </remarks>
    Text,

    /// <summary>
    /// YSON in binary form (passed/returned as byte[]).
    /// </summary>
    /// <remarks>
    /// Can't be used in the primary key.
    /// </remarks>
    Yson,

    /// <summary>
    /// JSON represented as text.
    /// </summary>
    /// <remarks>
    /// Doesn't support matching, can't be used in the primary key.
    /// </remarks>
    Json,

    /// <summary>
    /// JSON in an indexed binary representation.
    /// </summary>
    /// <remarks>
    /// Doesn't support matching, can't be used in the primary key.
    /// </remarks>
    JsonDocument,

    /// <summary>
    /// Universally unique identifier <a href="https://datatracker.ietf.org/doc/html/rfc4122">UUID</a>.
    /// </summary>
    Uuid,

    /// <summary>
    /// Date, precision to the day.
    /// </summary>
    /// <remarks>
    /// Range of values for all time types except Interval: From 00:00 01.01.1970 to 00:00 01.01.2106.
    /// Internal Date representation: Unsigned 16-bit integer
    /// </remarks>
    Date,

    /// <summary>
    /// Date/time, precision to the second.
    /// </summary>
    /// <remarks>
    /// Internal representation: Unsigned 32-bit integer.
    /// </remarks>
    DateTime,

    /// <summary>
    /// Date/time, precision to the microsecond.
    /// </summary>
    /// <remarks>
    /// Internal representation: Unsigned 64-bit integer.
    /// </remarks>
    Timestamp,

    /// <summary>
    /// Time interval (signed), precision to microseconds.
    /// </summary>
    /// <remarks>
    /// Value range: From -136 years to +136 years. Internal representation: Signed 64-bit integer.
    /// </remarks>
    Interval,

    /// <summary>
    /// Date with extended range, precision to the day.
    /// </summary>
    /// <remarks>
    /// Extended range date type that supports dates before 01.01.1970 and beyond the standard Date range.
    /// Provides better support for historical dates and future dates.
    /// </remarks>
    Date32,

    /// <summary>
    /// Date/time with extended range, precision to the second.
    /// </summary>
    /// <remarks>
    /// Extended range datetime type that supports date/time values before 01.01.1970 and
    /// beyond the standard Datetime range.
    /// Provides better support for historical timestamps and future timestamps.
    /// </remarks>
    Datetime64,

    /// <summary>
    /// Date/time with extended range, precision to the microsecond.
    /// </summary>
    /// <remarks>
    /// Extended range timestamp type that supports microsecond-precision timestamps
    /// before 01.01.1970 and beyond the standard Timestamp range. Provides better support for historical and future timestamps.
    /// </remarks>
    Timestamp64,

    /// <summary>
    /// Time interval with extended range, precision to microseconds.
    /// </summary>
    /// <remarks>
    /// Extended range interval type that supports larger time intervals
    /// beyond the standard Interval range.
    /// </remarks>
    Interval64
}

internal static class YdbDbTypeExtensions
{
    internal static YdbDbType ToYdbDbType(this DbType dbType) => dbType switch
    {
        DbType.Boolean => YdbDbType.Bool,
        DbType.String or
            DbType.AnsiString or
            DbType.AnsiStringFixedLength or
            DbType.StringFixedLength => YdbDbType.Text,
        DbType.Int64 => YdbDbType.Int64,
        DbType.Int32 => YdbDbType.Int32,
        DbType.Int16 => YdbDbType.Int16,
        DbType.SByte => YdbDbType.Int8,
        DbType.Byte => YdbDbType.UInt8,
        DbType.UInt16 => YdbDbType.UInt16,
        DbType.UInt32 => YdbDbType.UInt32,
        DbType.UInt64 => YdbDbType.UInt64,
        DbType.Single => YdbDbType.Float,
        DbType.Double => YdbDbType.Double,
        DbType.Decimal or DbType.Currency => YdbDbType.Decimal,
        DbType.Date => YdbDbType.Date,
        DbType.DateTime => YdbDbType.DateTime,
        DbType.DateTime2 or DbType.DateTimeOffset => YdbDbType.Timestamp,
        DbType.Guid => YdbDbType.Uuid,
        DbType.Binary => YdbDbType.Bytes,
        DbType.Object => YdbDbType.Unspecified,
        _ => throw new NotSupportedException($"Ydb don't supported this DbType: {dbType}")
    };
}
