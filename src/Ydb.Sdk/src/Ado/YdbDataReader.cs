using System.Data.Common;
using Google.Protobuf.Collections;
using Ydb.Issue;
using Ydb.Query;
using Ydb.Sdk.Ado.Internal;

namespace Ydb.Sdk.Ado;

/// <summary>
/// Provides a way of reading a forward-only stream of data rows from a YDB database. This class cannot be inherited.
/// </summary>
/// <remarks>
/// YdbDataReader provides a means of reading a forward-only stream of data rows from a YDB database.
/// It implements both synchronous and asynchronous data access methods, and supports streaming of large result sets.
/// The reader is optimized for YDB-specific data types and provides access to YDB-specific functionality.
/// </remarks>
// ReSharper disable once SwitchExpressionHandlesSomeKnownEnumValuesWithExceptionInDefault
public sealed class YdbDataReader : DbDataReader, IAsyncEnumerable<YdbDataRecord>
{
    private readonly IServerStream<ExecuteQueryResponsePart> _stream;
    private readonly YdbTransaction? _ydbTransaction;
    private readonly RepeatedField<IssueMessage> _issueMessagesInStream = new();
    private readonly Action<StatusCode> _onNotSuccessStatusCode;

    private int _currentRowIndex = -1;
    private long _resultSetIndex = -1;
    private ResultSet? _currentResultSet;

    private interface IMetadata
    {
        IReadOnlyDictionary<string, int> ColumnNameToOrdinal { get; }

        int FieldCount { get; }

        int RowsCount { get; }

        Column GetColumn(int ordinal);
    }

    private IMetadata ReaderMetadata { get; set; } = null!;

    private ResultSet CurrentResultSet => this switch
    {
        { ReaderState: State.ReadResultSet, _currentRowIndex: >= 0 } => _currentResultSet!,
        { ReaderState: State.Close } => throw new InvalidOperationException("The reader is closed"),
        _ => throw new InvalidOperationException("No row is available")
    };

    private IReadOnlyList<Ydb.Value> CurrentRow => CurrentResultSet.Rows[_currentRowIndex].Items;
    private int RowsCount => ReaderMetadata.RowsCount;

    private enum State
    {
        NewResultSet,
        ReadResultSet,
        IsConsumed,
        Close
    }

    private State ReaderState { get; set; }

    internal bool IsOpen => ReaderState is State.NewResultSet or State.ReadResultSet;

    private YdbDataReader(
        IServerStream<ExecuteQueryResponsePart> resultSetStream,
        Action<StatusCode> onNotSuccessStatusCode,
        YdbTransaction? ydbTransaction)
    {
        _stream = resultSetStream;
        _onNotSuccessStatusCode = onNotSuccessStatusCode;
        _ydbTransaction = ydbTransaction;
    }

    /// <summary>
    /// Creates a new instance of YdbDataReader from a result set stream.
    /// </summary>
    /// <param name="resultSetStream">The server stream containing query results.</param>
    /// <param name="onNotSuccessStatusCode">Callback for handling non-success status codes.</param>
    /// <param name="ydbTransaction">Optional transaction context.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the initialized YdbDataReader.</returns>
    internal static async Task<YdbDataReader> CreateYdbDataReader(
        IServerStream<ExecuteQueryResponsePart> resultSetStream,
        Action<StatusCode> onNotSuccessStatusCode,
        YdbTransaction? ydbTransaction = null,
        CancellationToken cancellationToken = default
    )
    {
        var ydbDataReader = new YdbDataReader(resultSetStream, onNotSuccessStatusCode, ydbTransaction);
        await ydbDataReader.Init(cancellationToken);

        return ydbDataReader;
    }

    private async Task Init(CancellationToken cancellationToken)
    {
        if (State.IsConsumed == await NextExecPart(cancellationToken))
        {
            throw new YdbException("YDB server closed the stream");
        }

        ReaderState = State.ReadResultSet;
    }

    /// <summary>
    /// Gets the value of the specified column as a Boolean.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public override bool GetBoolean(int ordinal) =>
        GetPrimitiveValue(Type.Types.PrimitiveTypeId.Bool, ordinal).GetBool();

    /// <summary>
    /// Gets the value of the specified column as a byte.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public override byte GetByte(int ordinal) =>
        GetPrimitiveValue(Type.Types.PrimitiveTypeId.Uint8, ordinal).GetUint8();

    /// <summary>
    /// Gets the value of the specified column as a signed byte.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column as a signed byte.</returns>
    public sbyte GetSByte(int ordinal) => GetPrimitiveValue(Type.Types.PrimitiveTypeId.Int8, ordinal).GetInt8();

    /// <summary>
    /// Gets the value of the specified column as a byte array.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column as a byte array.</returns>
    public byte[] GetBytes(int ordinal) => GetPrimitiveValue(Type.Types.PrimitiveTypeId.String, ordinal).GetBytes();

    public byte[] GetYson(int ordinal) => GetPrimitiveValue(Type.Types.PrimitiveTypeId.Yson, ordinal).GetYson();

    /// <summary>
    /// Reads a stream of bytes from the specified column offset into the buffer as an array.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <param name="dataOffset">The index within the field from which to start the read operation.</param>
    /// <param name="buffer">The buffer into which to read the stream of bytes.</param>
    /// <param name="bufferOffset">The index for buffer to start the read operation.</param>
    /// <param name="length">The maximum length to copy into the buffer.</param>
    /// <returns>The actual number of bytes read.</returns>
    /// <exception cref="IndexOutOfRangeException">Thrown when dataOffset, bufferOffset, or length are out of range.</exception>
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        var bytes = GetBytes(ordinal);

        CheckOffsets(dataOffset, buffer, bufferOffset, length);

        if (buffer == null)
        {
            return bytes.Length;
        }

        var copyCount = Math.Min(bytes.Length - dataOffset, length);

        if (copyCount < 0)
        {
            return 0;
        }

        Array.Copy(bytes, (int)dataOffset, buffer, bufferOffset, copyCount);

        return copyCount;
    }

    /// <summary>
    /// Gets the value of the specified column as a single character.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column as a character.</returns>
    /// <exception cref="System.InvalidCastException">
    /// Thrown when the string is empty or cannot be converted to a character.
    /// </exception>
    public override char GetChar(int ordinal)
    {
        var str = GetString(ordinal);

        return str.Length == 0 ? throw new InvalidCastException("Could not read char - string was empty") : str[0];
    }

    /// <summary>
    /// Reads a stream of characters from the specified column offset into the buffer as an array.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <param name="dataOffset">The index within the field from which to start the read operation.</param>
    /// <param name="buffer">The buffer into which to read the stream of characters.</param>
    /// <param name="bufferOffset">The index for buffer to start the read operation.</param>
    /// <param name="length">The maximum length to copy into the buffer.</param>
    /// <returns>The actual number of characters read.</returns>
    /// <exception cref="IndexOutOfRangeException">Thrown when dataOffset, bufferOffset, or length are out of range.</exception>
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        var chars = GetString(ordinal).ToCharArray();

        CheckOffsets(dataOffset, buffer, bufferOffset, length);

        if (buffer == null)
        {
            return chars.Length;
        }

        var copyCount = Math.Min(chars.Length - dataOffset, length);

        if (copyCount < 0)
        {
            return 0;
        }

        Array.Copy(chars, (int)dataOffset, buffer, bufferOffset, copyCount);

        return copyCount;
    }

    private static void CheckOffsets<T>(long dataOffset, T[]? buffer, int bufferOffset, int length)
    {
        if (dataOffset is < 0 or > int.MaxValue)
        {
            throw new IndexOutOfRangeException($"dataOffset must be between 0 and {int.MaxValue}");
        }

        if (buffer != null && (bufferOffset < 0 || bufferOffset > buffer.Length))
        {
            throw new IndexOutOfRangeException($"bufferOffset must be between 0 and {buffer.Length}");
        }

        if (buffer != null && length < 0)
        {
            throw new IndexOutOfRangeException($"length must be between 0 and {buffer.Length}");
        }

        if (buffer != null && length > buffer.Length - bufferOffset)
        {
            throw new IndexOutOfRangeException($"bufferOffset must be between 0 and {buffer.Length - length}");
        }
    }

    /// <summary>
    /// Gets the name of the data type of the specified column.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The string representing the data type of the specified column.</returns>
    public override string GetDataTypeName(int ordinal) => ReaderMetadata.GetColumn(ordinal).Type.YqlTableType();

    /// <summary>
    /// Gets the value of the specified column as a DateTime object.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public override DateTime GetDateTime(int ordinal)
    {
        var type = UnwrapColumnType(ordinal);

        return type.TypeId switch
        {
            Type.Types.PrimitiveTypeId.Timestamp => CurrentRow[ordinal].GetTimestamp(),
            Type.Types.PrimitiveTypeId.Datetime => CurrentRow[ordinal].GetDatetime(),
            Type.Types.PrimitiveTypeId.Date => CurrentRow[ordinal].GetDate(),
            Type.Types.PrimitiveTypeId.Timestamp64 => CurrentRow[ordinal].GetTimestamp64(),
            Type.Types.PrimitiveTypeId.Datetime64 => CurrentRow[ordinal].GetDatetime64(),
            Type.Types.PrimitiveTypeId.Date32 => CurrentRow[ordinal].GetDate32(),
            _ => throw InvalidCastException<DateTime>(ordinal)
        };
    }

    /// <summary>
    /// Gets the value of the specified column as a TimeSpan object.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column as a TimeSpan.</returns>
    public TimeSpan GetInterval(int ordinal)
    {
        var type = UnwrapColumnType(ordinal);

        return type.TypeId switch
        {
            Type.Types.PrimitiveTypeId.Interval => CurrentRow[ordinal].GetInterval(),
            Type.Types.PrimitiveTypeId.Interval64 => CurrentRow[ordinal].GetInterval64(),
            _ => throw InvalidCastException<TimeSpan>(ordinal)
        };
    }

    /// <summary>
    /// Gets the value of the specified column as a Decimal object.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public override decimal GetDecimal(int ordinal)
    {
        var type = UnwrapColumnType(ordinal);

        return type.TypeCase == Type.TypeOneofCase.DecimalType
            ? CurrentRow[ordinal].GetDecimal((byte)type.DecimalType.Scale)
            : throw InvalidCastException(Type.TypeOneofCase.DecimalType, ordinal);
    }

    /// <summary>
    /// Gets the value of the specified column as a double-precision floating point number.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public override double GetDouble(int ordinal)
    {
        var type = UnwrapColumnType(ordinal);

        return type.TypeId switch
        {
            Type.Types.PrimitiveTypeId.Double => CurrentRow[ordinal].GetDouble(),
            Type.Types.PrimitiveTypeId.Float => CurrentRow[ordinal].GetFloat(),
            _ => throw InvalidCastException<double>(ordinal)
        };
    }

    /// <summary>
    /// Gets the value of the specified column as the requested type.
    /// </summary>
    /// <typeparam name="T">The type of the value to return.</typeparam>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public override T GetFieldValue<T>(int ordinal)
    {
        if (typeof(T) == typeof(TextReader))
        {
            return (T)(object)GetTextReader(ordinal);
        }

        if (typeof(T) == typeof(Stream))
        {
            return (T)(object)GetStream(ordinal);
        }

        if (typeof(T) == typeof(char))
        {
            return (T)(object)GetChar(ordinal);
        }

        return base.GetFieldValue<T>(ordinal);
    }

    /// <summary>
    /// Gets the Type that is the data type of the object.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The Type that is the data type of the object.</returns>
    public override System.Type GetFieldType(int ordinal)
    {
        var type = ReaderMetadata.GetColumn(ordinal).Type;

        if (type.TypeCase == Type.TypeOneofCase.OptionalType)
        {
            type = type.OptionalType.Item;
        }

        if (type.TypeCase == Type.TypeOneofCase.DecimalType)
        {
            return typeof(decimal);
        }

        return type.TypeId switch
        {
            Type.Types.PrimitiveTypeId.Date
                or Type.Types.PrimitiveTypeId.Date32
                or Type.Types.PrimitiveTypeId.Datetime
                or Type.Types.PrimitiveTypeId.Datetime64
                or Type.Types.PrimitiveTypeId.Timestamp
                or Type.Types.PrimitiveTypeId.Timestamp64 => typeof(DateTime),
            Type.Types.PrimitiveTypeId.Bool => typeof(bool),
            Type.Types.PrimitiveTypeId.Int8 => typeof(sbyte),
            Type.Types.PrimitiveTypeId.Uint8 => typeof(byte),
            Type.Types.PrimitiveTypeId.Int16 => typeof(short),
            Type.Types.PrimitiveTypeId.Uint16 => typeof(ushort),
            Type.Types.PrimitiveTypeId.Int32 => typeof(int),
            Type.Types.PrimitiveTypeId.Uint32 => typeof(uint),
            Type.Types.PrimitiveTypeId.Int64 => typeof(long),
            Type.Types.PrimitiveTypeId.Uint64 => typeof(ulong),
            Type.Types.PrimitiveTypeId.Float => typeof(float),
            Type.Types.PrimitiveTypeId.Double => typeof(double),
            Type.Types.PrimitiveTypeId.Interval => typeof(TimeSpan),
            Type.Types.PrimitiveTypeId.Utf8
                or Type.Types.PrimitiveTypeId.JsonDocument
                or Type.Types.PrimitiveTypeId.Json => typeof(string),
            Type.Types.PrimitiveTypeId.String => typeof(byte[]),
            Type.Types.PrimitiveTypeId.Yson => typeof(byte[]),
            Type.Types.PrimitiveTypeId.Uuid => typeof(Guid),
            _ => throw new YdbException($"Unsupported ydb type {type}")
        };
    }

    /// <summary>
    /// Gets the value of the specified column as a single-precision floating point number.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public override float GetFloat(int ordinal) =>
        GetPrimitiveValue(Type.Types.PrimitiveTypeId.Float, ordinal).GetFloat();

    /// <summary>
    /// Gets the value of the specified column as a globally unique identifier (GUID).
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public override Guid GetGuid(int ordinal) => GetPrimitiveValue(Type.Types.PrimitiveTypeId.Uuid, ordinal).GetUuid();

    /// <summary>
    /// Gets the value of the specified column as a 16-bit signed integer.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public override short GetInt16(int ordinal)
    {
        var type = UnwrapColumnType(ordinal);

        return type.TypeId switch
        {
            Type.Types.PrimitiveTypeId.Int16 => CurrentRow[ordinal].GetInt16(),
            Type.Types.PrimitiveTypeId.Int8 => CurrentRow[ordinal].GetInt8(),
            Type.Types.PrimitiveTypeId.Uint8 => CurrentRow[ordinal].GetUint8(),
            _ => throw InvalidCastException<short>(ordinal)
        };
    }

    /// <summary>
    /// Gets the value of the specified column as a 16-bit unsigned integer.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public ushort GetUint16(int ordinal)
    {
        var type = UnwrapColumnType(ordinal);

        return type.TypeId switch
        {
            Type.Types.PrimitiveTypeId.Uint16 => CurrentRow[ordinal].GetUint16(),
            Type.Types.PrimitiveTypeId.Uint8 => CurrentRow[ordinal].GetUint8(),
            _ => throw InvalidCastException<ushort>(ordinal)
        };
    }

    /// <summary>
    /// Gets the value of the specified column as a 32-bit signed integer.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public override int GetInt32(int ordinal)
    {
        var type = UnwrapColumnType(ordinal);

        return type.TypeId switch
        {
            Type.Types.PrimitiveTypeId.Int32 => CurrentRow[ordinal].GetInt32(),
            Type.Types.PrimitiveTypeId.Int16 => CurrentRow[ordinal].GetInt16(),
            Type.Types.PrimitiveTypeId.Int8 => CurrentRow[ordinal].GetInt8(),
            Type.Types.PrimitiveTypeId.Uint16 => CurrentRow[ordinal].GetUint16(),
            Type.Types.PrimitiveTypeId.Uint8 => CurrentRow[ordinal].GetUint8(),
            _ => throw InvalidCastException<int>(ordinal)
        };
    }

    /// <summary>
    /// Gets the value of the specified column as a 32-bit unsigned integer.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    /// <summary>
    /// Gets the value of the specified column as a 32-bit unsigned integer.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column as a 32-bit unsigned integer.</returns>
    public uint GetUint32(int ordinal)
    {
        var type = UnwrapColumnType(ordinal);

        return type.TypeId switch
        {
            Type.Types.PrimitiveTypeId.Uint32 => CurrentRow[ordinal].GetUint32(),
            Type.Types.PrimitiveTypeId.Uint16 => CurrentRow[ordinal].GetUint16(),
            Type.Types.PrimitiveTypeId.Uint8 => CurrentRow[ordinal].GetUint8(),
            _ => throw InvalidCastException<uint>(ordinal)
        };
    }

    /// <summary>
    /// Gets the value of the specified column as a 64-bit signed integer.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public override long GetInt64(int ordinal)
    {
        var type = UnwrapColumnType(ordinal);

        return type.TypeId switch
        {
            Type.Types.PrimitiveTypeId.Int64 => CurrentRow[ordinal].GetInt64(),
            Type.Types.PrimitiveTypeId.Int32 => CurrentRow[ordinal].GetInt32(),
            Type.Types.PrimitiveTypeId.Int16 => CurrentRow[ordinal].GetInt16(),
            Type.Types.PrimitiveTypeId.Int8 => CurrentRow[ordinal].GetInt8(),
            Type.Types.PrimitiveTypeId.Uint32 => CurrentRow[ordinal].GetUint32(),
            Type.Types.PrimitiveTypeId.Uint16 => CurrentRow[ordinal].GetUint16(),
            Type.Types.PrimitiveTypeId.Uint8 => CurrentRow[ordinal].GetUint8(),
            _ => throw InvalidCastException<long>(ordinal)
        };
    }

    /// <summary>
    /// Gets the value of the specified column as a 64-bit unsigned integer.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public ulong GetUint64(int ordinal)
    {
        var type = UnwrapColumnType(ordinal);

        return type.TypeId switch
        {
            Type.Types.PrimitiveTypeId.Uint64 => CurrentRow[ordinal].GetUint64(),
            Type.Types.PrimitiveTypeId.Uint32 => CurrentRow[ordinal].GetUint32(),
            Type.Types.PrimitiveTypeId.Uint16 => CurrentRow[ordinal].GetUint16(),
            Type.Types.PrimitiveTypeId.Uint8 => CurrentRow[ordinal].GetUint8(),
            _ => throw InvalidCastException<ulong>(ordinal)
        };
    }

    /// <summary>
    /// Gets the name of the specified column.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The name of the specified column.</returns>
    public override string GetName(int ordinal) => ReaderMetadata.GetColumn(ordinal).Name;

    /// <summary>
    /// Gets the column ordinal given the name of the column.
    /// </summary>
    /// <param name="name">The name of the column.</param>
    /// <returns>The zero-based column ordinal.</returns>
    /// <exception cref="IndexOutOfRangeException">Thrown when the column name is not found.</exception>
    public override int GetOrdinal(string name)
    {
        if (ReaderMetadata.ColumnNameToOrdinal.TryGetValue(name, out var ordinal))
        {
            return ordinal;
        }

        throw new IndexOutOfRangeException($"Field not found in row: {name}");
    }

    /// <summary>
    /// Gets the value of the specified column as a string.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public override string GetString(int ordinal) =>
        GetPrimitiveValue(Type.Types.PrimitiveTypeId.Utf8, ordinal).GetText();

    /// <summary>
    /// Gets the value of the specified column as a TextReader.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>A TextReader containing the column value.</returns>
    public override TextReader GetTextReader(int ordinal) => new StringReader(GetString(ordinal));

    /// <summary>
    /// Gets the value of the specified column as a JSON string.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column as JSON.</returns>
    public string GetJson(int ordinal) => GetPrimitiveValue(Type.Types.PrimitiveTypeId.Json, ordinal).GetJson();

    /// <summary>
    /// Gets the value of the specified column as a JSON document string.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column as a JSON document.</returns>
    public string GetJsonDocument(int ordinal) =>
        GetPrimitiveValue(Type.Types.PrimitiveTypeId.JsonDocument, ordinal).GetJsonDocument();

    /// <summary>
    /// Gets the value of the specified column in its native format.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column in its native format.</returns>
    public override object GetValue(int ordinal)
    {
        var type = GetColumnType(ordinal);
        var ydbValue = CurrentRow[ordinal];

        if (ydbValue.IsNull())
        {
            return DBNull.Value;
        }

        if (type.TypeCase == Type.TypeOneofCase.OptionalType)
        {
            type = type.OptionalType.Item;
        }

        if (type.TypeCase == Type.TypeOneofCase.DecimalType)
        {
            return ydbValue.GetDecimal(type.DecimalType.Scale);
        }

        return type.TypeId switch
        {
            Type.Types.PrimitiveTypeId.Date => ydbValue.GetDate(),
            Type.Types.PrimitiveTypeId.Date32 => ydbValue.GetDate32(),
            Type.Types.PrimitiveTypeId.Datetime => ydbValue.GetDatetime(),
            Type.Types.PrimitiveTypeId.Datetime64 => ydbValue.GetDatetime64(),
            Type.Types.PrimitiveTypeId.Timestamp => ydbValue.GetTimestamp(),
            Type.Types.PrimitiveTypeId.Timestamp64 => ydbValue.GetTimestamp64(),
            Type.Types.PrimitiveTypeId.Bool => ydbValue.GetBool(),
            Type.Types.PrimitiveTypeId.Int8 => ydbValue.GetInt8(),
            Type.Types.PrimitiveTypeId.Uint8 => ydbValue.GetUint8(),
            Type.Types.PrimitiveTypeId.Int16 => ydbValue.GetInt16(),
            Type.Types.PrimitiveTypeId.Uint16 => ydbValue.GetUint16(),
            Type.Types.PrimitiveTypeId.Int32 => ydbValue.GetInt32(),
            Type.Types.PrimitiveTypeId.Uint32 => ydbValue.GetUint32(),
            Type.Types.PrimitiveTypeId.Int64 => ydbValue.GetInt64(),
            Type.Types.PrimitiveTypeId.Uint64 => ydbValue.GetUint64(),
            Type.Types.PrimitiveTypeId.Float => ydbValue.GetFloat(),
            Type.Types.PrimitiveTypeId.Double => ydbValue.GetDouble(),
            Type.Types.PrimitiveTypeId.Interval => ydbValue.GetInterval(),
            Type.Types.PrimitiveTypeId.Utf8 => ydbValue.GetText(),
            Type.Types.PrimitiveTypeId.Json => ydbValue.GetJson(),
            Type.Types.PrimitiveTypeId.JsonDocument => ydbValue.GetJsonDocument(),
            Type.Types.PrimitiveTypeId.Yson => ydbValue.GetYson(),
            Type.Types.PrimitiveTypeId.String => ydbValue.GetBytes(),
            Type.Types.PrimitiveTypeId.Uuid => ydbValue.GetUuid(),
            _ => throw new YdbException($"Unsupported ydb type {GetColumnType(ordinal)}")
        };
    }

    /// <summary>
    /// Populates an array of objects with the column values of the current row.
    /// </summary>
    /// <param name="values">An array of Object into which to copy the attribute columns.</param>
    /// <returns>The number of instances of Object in the array.</returns>
    public override int GetValues(object[] values)
    {
        ArgumentNullException.ThrowIfNull(values);
        if (FieldCount == 0)
        {
            throw new InvalidOperationException("No resultset is currently being traversed");
        }

        var count = Math.Min(FieldCount, values.Length);
        for (var i = 0; i < count; i++)
            values[i] = GetValue(i);
        return count;
    }

    /// <summary>
    /// Gets a value that indicates whether the specified column contains null values.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>true if the specified column is equivalent to DBNull; otherwise, false.</returns>
    public override bool IsDBNull(int ordinal) => CurrentRow[ordinal].IsNull();

    /// <summary>
    /// Gets the number of columns in the current row.
    /// </summary>
    public override int FieldCount => ReaderMetadata.FieldCount;

    /// <summary>
    /// Gets the value of the specified column in its native format given the column ordinal.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column in its native format.</returns>
    public override object this[int ordinal] => GetValue(ordinal);

    /// <summary>
    /// Gets the value of the specified column in its native format given the column name.
    /// </summary>
    /// <param name="name">The name of the column.</param>
    /// <returns>The value of the specified column in its native format.</returns>
    public override object this[string name] => GetValue(GetOrdinal(name));

    /// <summary>
    /// Gets the number of rows changed, inserted, or deleted by execution of the SQL statement.
    /// </summary>
    /// <remarks>For YDB, this always returns -1 as the number of affected records is not available.</remarks>
    public override int RecordsAffected => -1;

    /// <summary>
    /// Gets a value that indicates whether the data reader contains one or more rows.
    /// </summary>
    public override bool HasRows => ReaderMetadata.RowsCount > 0;

    /// <summary>
    /// Gets a value that indicates whether the data reader is closed.
    /// </summary>
    public override bool IsClosed => ReaderState == State.Close;

    /// <summary>
    /// Advances the data reader to the next result set.
    /// </summary>
    /// <returns>true if there are more result sets; otherwise, false.</returns>
    public override bool NextResult() => NextResultAsync().GetAwaiter().GetResult();

    /// <summary>
    /// Advances the data reader to the next record.
    /// </summary>
    /// <returns>true if there are more rows; otherwise, false.</returns>
    public override bool Read() => ReadAsync().GetAwaiter().GetResult();

    /// <summary>
    /// Asynchronously advances the data reader to the next result set.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous operation. The task result indicates whether there are more result sets.</returns>
    public override async Task<bool> NextResultAsync(CancellationToken cancellationToken)
    {
        ThrowIfClosed();

        ReaderState = ReaderState switch
        {
            State.IsConsumed => State.IsConsumed,
            State.NewResultSet => State.ReadResultSet,
            State.ReadResultSet => await new Func<Task<State>>(async () =>
            {
                State state;
                while ((state = await NextExecPart(cancellationToken)) == State.ReadResultSet)
                {
                }

                return state == State.NewResultSet ? State.ReadResultSet : state;
            })(),
            State.Close => State.Close, // not invoke
            _ => throw new ArgumentOutOfRangeException(ReaderState.ToString())
        };

        return ReaderState != State.IsConsumed;
    }

    /// <summary>
    /// Asynchronously advances the data reader to the next record.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous operation. The task result indicates whether there are more rows.</returns>
    public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        ThrowIfClosed();

        if (ReaderState == State.IsConsumed)
        {
            return false;
        }

        if (++_currentRowIndex < RowsCount)
        {
            return true;
        }

        while ((ReaderState = await NextExecPart(cancellationToken)) == State.ReadResultSet) // reset _currentRowIndex
        {
            if (++_currentRowIndex < RowsCount)
            {
                return true;
            }
        }

        return false;
    }

    private void ThrowIfClosed()
    {
        if (ReaderState == State.Close)
        {
            throw new InvalidOperationException("The reader is closed");
        }
    }

    /// <summary>
    /// Gets a value indicating the depth of nesting for the current row.
    /// </summary>
    /// <remarks>
    /// For YdbDataReader, this always returns 0 as YDB does not support nested result sets.
    /// </remarks>
    public override int Depth => 0;

    /// <summary>
    /// Returns an enumerator that iterates through the <see cref="YdbDataReader"/>.
    /// </summary>
    /// <returns>An enumerator that can be used to iterate through the <see cref="YdbDataRecord"/> collection.</returns>
    /// <remarks>
    /// This method provides synchronous enumeration over the data reader records.
    /// Each iteration advances the reader to the next row.
    /// </remarks>
    public override IEnumerator<YdbDataRecord> GetEnumerator()
    {
        while (Read())
        {
            yield return new YdbDataRecord(this);
        }
    }

    /// <summary>
    /// Asynchronously closes the <see cref="YdbDataReader"/> object.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// This method closes the reader and releases any resources associated with it.
    /// If the reader is closed during a transaction, the transaction will be marked as failed.
    /// </remarks>
    public override async Task CloseAsync()
    {
        if (ReaderState == State.Close)
        {
            return;
        }

        var isConsumed = ReaderState == State.IsConsumed || (!await ReadAsync() && ReaderState == State.IsConsumed);
        ReaderMetadata = CloseMetadata.Instance;
        ReaderState = State.Close;

        if (isConsumed)
        {
            return;
        }

        _onNotSuccessStatusCode(StatusCode.SessionBusy);
        _stream.Dispose();

        if (_ydbTransaction != null)
        {
            _ydbTransaction.Failed = true;

            throw new YdbException("YdbDataReader was closed during transaction execution. Transaction is broken!");
        }
    }

    /// <summary>
    /// Closes the <see cref="YdbDataReader"/> object.
    /// </summary>
    /// <remarks>
    /// This method closes the reader and releases any resources associated with it.
    /// If the reader is closed during a transaction, the transaction will be marked as failed.
    /// </remarks>
    public override void Close() => CloseAsync().GetAwaiter().GetResult();

    private Type UnwrapColumnType(int ordinal)
    {
        var type = GetColumnType(ordinal);

        if (CurrentRow[ordinal].IsNull())
            throw new InvalidCastException("Field is null.");

        return type.TypeCase == Type.TypeOneofCase.OptionalType ? type.OptionalType.Item : type;
    }

    private Type GetColumnType(int ordinal) => ReaderMetadata.GetColumn(ordinal).Type;

    private Ydb.Value GetPrimitiveValue(Type.Types.PrimitiveTypeId primitiveTypeId, int ordinal)
    {
        var type = UnwrapColumnType(ordinal);
        var ydbValue = CurrentRow[ordinal];

        return type.TypeId == primitiveTypeId ? ydbValue : throw InvalidCastException(primitiveTypeId, ordinal);
    }

    private async ValueTask<State> NextExecPart(CancellationToken cancellationToken)
    {
        try
        {
            _currentRowIndex = -1;

            if (!await _stream.MoveNextAsync(cancellationToken))
            {
                return State.IsConsumed;
            }

            var part = _stream.Current;

            _issueMessagesInStream.AddRange(part.Issues);

            if (part.Status.IsNotSuccess())
            {
                while (await _stream.MoveNextAsync(cancellationToken))
                {
                    _issueMessagesInStream.AddRange(_stream.Current.Issues);
                }

                throw YdbException.FromServer(part.Status, _issueMessagesInStream);
            }

            _currentResultSet = part.ResultSet;
            ReaderMetadata = _currentResultSet != null ? new Metadata(_currentResultSet) : EmptyMetadata.Instance;

            if (_ydbTransaction != null && part.TxMeta != null)
            {
                _ydbTransaction.TxId ??= part.TxMeta.Id;
            }

            if (part.ResultSetIndex <= _resultSetIndex)
            {
                return State.ReadResultSet;
            }

            _resultSetIndex = part.ResultSetIndex;

            return State.NewResultSet;
        }
        catch (YdbException e)
        {
            OnFailReadStream();

            _onNotSuccessStatusCode(e.Code);

            throw;
        }
    }

    private void OnFailReadStream()
    {
        ReaderState = State.Close;

        if (_ydbTransaction != null)
        {
            _ydbTransaction.Failed = true;
        }
    }

    /// <summary>
    /// Asynchronously releases the unmanaged resources used by the YdbDataReader.
    /// </summary>
    /// <returns>A ValueTask representing the asynchronous disposal operation.</returns>
    /// <remarks>
    /// This method closes the reader and releases any resources associated with it.
    /// </remarks>
    public override async ValueTask DisposeAsync() => await CloseAsync();

    /// <summary>
    /// Returns an async enumerator that iterates through the YdbDataReader asynchronously.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the enumeration.</param>
    /// <returns>An async enumerator that can be used to iterate through the YdbDataRecord collection.</returns>
    /// <remarks>
    /// This method provides asynchronous enumeration over the data reader records.
    /// Each iteration advances the reader to the next row asynchronously.
    /// </remarks>
    public async IAsyncEnumerator<YdbDataRecord> GetAsyncEnumerator(CancellationToken cancellationToken = new())
    {
        while (await ReadAsync(cancellationToken))
        {
            yield return new YdbDataRecord(this);
        }
    }

    private class EmptyMetadata : IMetadata
    {
        public static readonly IMetadata Instance = new EmptyMetadata();

        private EmptyMetadata()
        {
        }

        public IReadOnlyDictionary<string, int> ColumnNameToOrdinal =>
            throw new InvalidOperationException("No resultset is currently being traversed");

        public int FieldCount => 0;
        public int RowsCount => 0;

        public Column GetColumn(int ordinal) =>
            throw new InvalidOperationException("No resultset is currently being traversed");
    }

    private class CloseMetadata : IMetadata
    {
        public static readonly IMetadata Instance = new CloseMetadata();

        private CloseMetadata()
        {
        }

        public IReadOnlyDictionary<string, int> ColumnNameToOrdinal =>
            throw new InvalidOperationException("The reader is closed");

        public int FieldCount => throw new InvalidOperationException("The reader is closed");
        public int RowsCount => 0;

        public Column GetColumn(int ordinal) => throw new InvalidOperationException("The reader is closed");
    }

    private class Metadata : IMetadata
    {
        private IReadOnlyList<Column> Columns { get; }

        public IReadOnlyDictionary<string, int> ColumnNameToOrdinal { get; }
        public int FieldCount { get; }
        public int RowsCount { get; }

        public Metadata(ResultSet resultSet)
        {
            Columns = resultSet.Columns;
            ColumnNameToOrdinal = Columns
                .Select((c, idx) => (c.Name, Index: idx))
                .ToDictionary(t => t.Name, t => t.Index);
            RowsCount = resultSet.Rows.Count;
            FieldCount = resultSet.Columns.Count;
        }

        public Column GetColumn(int ordinal)
        {
            if (ordinal < 0 || ordinal >= FieldCount)
            {
                throw new IndexOutOfRangeException("Ordinal must be between 0 and " + (FieldCount - 1));
            }

            return Columns[ordinal];
        }
    }

    private InvalidCastException InvalidCastException<T>(int ordinal) =>
        new($"Field YDB type {GetColumnType(ordinal)} can't be cast to {typeof(T)} type.");

    private InvalidCastException InvalidCastException(Type.Types.PrimitiveTypeId expectedType, int ordinal) =>
        new($"Invalid type of YDB value, expected primitive typeId: {expectedType}, actual: {GetColumnType(ordinal)}.");

    private InvalidCastException InvalidCastException(Type.TypeOneofCase expectedType, int ordinal)
        => new($"Invalid type of YDB value, expected: {expectedType}, actual: {GetColumnType(ordinal)}.");
}
