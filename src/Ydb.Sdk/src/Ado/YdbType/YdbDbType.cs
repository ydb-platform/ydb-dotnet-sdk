using System.Data;

namespace Ydb.Sdk.Ado.YdbType;

/// <summary>
/// See <a href="https://ydb.tech/docs/en/yql/reference/types/primitive">YDB Primitive Types</a> for details.
/// </summary>
public enum YdbDbType
{
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
    /// A real number with the specified precision, 16 bytes in size.
    /// Precision is the maximum total number of decimal digits stored and can range from 1 to 35.
    /// Scale is the maximum number of decimal digits stored
    /// to the right of the decimal point and can range from 0 to the precision value.
    /// </summary>
    /// <remarks>
    /// Precision and Scale are specified in the <see cref="Ydb.Sdk.Ado.YdbParameter"/>.
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
    /// Can't be used in the primary key.
    /// </remarks>
    Interval,

    Date32,

    Datetime64,

    Timestamp64,

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
