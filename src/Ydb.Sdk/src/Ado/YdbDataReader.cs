using System.Data.Common;
using Google.Protobuf.Collections;
using Ydb.Issue;
using Ydb.Query;
using Ydb.Sdk.Ado.Internal;

namespace Ydb.Sdk.Ado;

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

    public override bool GetBoolean(int ordinal) =>
        GetPrimitiveValue(Type.Types.PrimitiveTypeId.Bool, ordinal).GetBool();

    public override byte GetByte(int ordinal) =>
        GetPrimitiveValue(Type.Types.PrimitiveTypeId.Uint8, ordinal).GetUint8();

    public sbyte GetSByte(int ordinal) => GetPrimitiveValue(Type.Types.PrimitiveTypeId.Int8, ordinal).GetInt8();

    public byte[] GetBytes(int ordinal) => GetPrimitiveValue(Type.Types.PrimitiveTypeId.String, ordinal).GetBytes();

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

    public override char GetChar(int ordinal)
    {
        var str = GetString(ordinal);

        return str.Length == 0 ? throw new InvalidCastException("Could not read char - string was empty") : str[0];
    }

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

    public override string GetDataTypeName(int ordinal) => ReaderMetadata.GetColumn(ordinal).Type.YqlTableType();

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

    public override decimal GetDecimal(int ordinal)
    {
        var type = UnwrapColumnType(ordinal);

        return type.TypeCase == Type.TypeOneofCase.DecimalType
            ? CurrentRow[ordinal].GetDecimal((byte)type.DecimalType.Scale)
            : throw InvalidCastException(Type.TypeOneofCase.DecimalType, ordinal);
    }

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
            Type.Types.PrimitiveTypeId.Uuid => typeof(Guid),
            _ => throw new YdbException($"Unsupported ydb type {type}")
        };
    }

    public override float GetFloat(int ordinal) =>
        GetPrimitiveValue(Type.Types.PrimitiveTypeId.Float, ordinal).GetFloat();

    public override Guid GetGuid(int ordinal) => GetPrimitiveValue(Type.Types.PrimitiveTypeId.Uuid, ordinal).GetUuid();

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

    public override string GetName(int ordinal) => ReaderMetadata.GetColumn(ordinal).Name;

    public override int GetOrdinal(string name)
    {
        if (ReaderMetadata.ColumnNameToOrdinal.TryGetValue(name, out var ordinal))
        {
            return ordinal;
        }

        throw new IndexOutOfRangeException($"Field not found in row: {name}");
    }

    public override string GetString(int ordinal) =>
        GetPrimitiveValue(Type.Types.PrimitiveTypeId.Utf8, ordinal).GetText();

    public override TextReader GetTextReader(int ordinal) => new StringReader(GetString(ordinal));

    public string GetJson(int ordinal) => GetPrimitiveValue(Type.Types.PrimitiveTypeId.Json, ordinal).GetJson();

    public string GetJsonDocument(int ordinal) =>
        GetPrimitiveValue(Type.Types.PrimitiveTypeId.JsonDocument, ordinal).GetJsonDocument();

    public override object GetValue(int ordinal)
    {
        var type = GetColumnType(ordinal);
        var ydbValue = CurrentRow[ordinal];

        if (type.IsNull())
        {
            return DBNull.Value;
        }

        if (type.IsOptional())
        {
            if (ydbValue.IsNull())
            {
                return DBNull.Value;
            }

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
            Type.Types.PrimitiveTypeId.String => ydbValue.GetBytes(),
            Type.Types.PrimitiveTypeId.Uuid => ydbValue.GetUuid(),
            _ => throw new YdbException($"Unsupported ydb type {GetColumnType(ordinal)}")
        };
    }

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

    public override bool IsDBNull(int ordinal)
    {
        var type = GetColumnType(ordinal);

        return (type.IsOptional() && CurrentRow[ordinal].IsNull()) || type.IsNull();
    }

    public override int FieldCount => ReaderMetadata.FieldCount;
    public override object this[int ordinal] => GetValue(ordinal);
    public override object this[string name] => GetValue(GetOrdinal(name));
    public override int RecordsAffected => -1;
    public override bool HasRows => ReaderMetadata.RowsCount > 0;
    public override bool IsClosed => ReaderState == State.Close;

    public override bool NextResult() => NextResultAsync().GetAwaiter().GetResult();

    public override bool Read() => ReadAsync().GetAwaiter().GetResult();

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

    public override int Depth => 0;

    public override IEnumerator<YdbDataRecord> GetEnumerator()
    {
        while (Read())
        {
            yield return new YdbDataRecord(this);
        }
    }

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

    public override void Close() => CloseAsync().GetAwaiter().GetResult();

    private Type UnwrapColumnType(int ordinal)
    {
        var type = GetColumnType(ordinal);

        if (type.IsNull())
            throw new InvalidCastException("Field is null.");

        if (!type.IsOptional())
            return type;

        if (CurrentRow[ordinal].IsNull())
            throw new InvalidCastException("Field is null.");

        return type.OptionalType.Item;
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

    public override async ValueTask DisposeAsync() => await CloseAsync();

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
            ColumnNameToOrdinal = ColumnNameToOrdinal = Columns
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
