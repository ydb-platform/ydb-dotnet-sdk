using System.Data.Common;
using Google.Protobuf.Collections;
using Ydb.Issue;
using Ydb.Query;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado;

public sealed class YdbDataReader : DbDataReader, IAsyncEnumerable<YdbDataRecord>
{
    private readonly IAsyncEnumerator<ExecuteQueryResponsePart> _stream;
    private readonly YdbTransaction? _ydbTransaction;
    private readonly RepeatedField<IssueMessage> _issueMessagesInStream = new();
    private readonly Action<Status> _onNotSuccessStatus;

    private int _currentRowIndex = -1;
    private long _resultSetIndex = -1;
    private Value.ResultSet? _currentResultSet;

    private interface IMetadata
    {
        IReadOnlyDictionary<string, int> ColumnNameToOrdinal { get; }

        int FieldCount { get; }

        int RowsCount { get; }

        Value.ResultSet.Column GetColumn(int ordinal);
    }

    private IMetadata ReaderMetadata { get; set; } = null!;

    private Value.ResultSet CurrentResultSet => this switch
    {
        { ReaderState: State.ReadResultSet, _currentRowIndex: >= 0 } => _currentResultSet!,
        { ReaderState: State.Close } => throw new InvalidOperationException("The reader is closed"),
        _ => throw new InvalidOperationException("No row is available")
    };

    private Value.ResultSet.Row CurrentRow => CurrentResultSet.Rows[_currentRowIndex];
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
        IAsyncEnumerator<ExecuteQueryResponsePart> resultSetStream,
        Action<Status> onNotSuccessStatus,
        YdbTransaction? ydbTransaction)
    {
        _stream = resultSetStream;
        _onNotSuccessStatus = onNotSuccessStatus;
        _ydbTransaction = ydbTransaction;
    }

    internal static async Task<YdbDataReader> CreateYdbDataReader(
        IAsyncEnumerator<ExecuteQueryResponsePart> resultSetStream,
        Action<Status> onStatus,
        YdbTransaction? ydbTransaction = null)
    {
        var ydbDataReader = new YdbDataReader(resultSetStream, onStatus, ydbTransaction);
        await ydbDataReader.Init();

        return ydbDataReader;
    }

    private async Task Init()
    {
        if (State.IsConsumed == await NextExecPart())
        {
            throw new YdbException("YDB server closed the stream");
        }

        ReaderState = State.ReadResultSet;
    }

    public override bool GetBoolean(int ordinal)
    {
        return GetFieldYdbValue(ordinal).GetBool();
    }

    public override byte GetByte(int ordinal)
    {
        return GetFieldYdbValue(ordinal).GetUint8();
    }

    public sbyte GetSByte(int ordinal)
    {
        return GetFieldYdbValue(ordinal).GetInt8();
    }

    // ReSharper disable once MemberCanBePrivate.Global
    public byte[] GetBytes(int ordinal)
    {
        return GetFieldYdbValue(ordinal).GetString();
    }

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

    public override string GetDataTypeName(int ordinal)
    {
        return ReaderMetadata.GetColumn(ordinal).Type.YqlTableType();
    }

    public override DateTime GetDateTime(int ordinal)
    {
        var ydbValue = GetFieldYdbValue(ordinal);

        return ydbValue.TypeId switch
        {
            YdbTypeId.Timestamp => ydbValue.GetTimestamp(),
            YdbTypeId.Datetime => ydbValue.GetDatetime(),
            YdbTypeId.Date => ydbValue.GetDate(),
            _ => ThrowHelper.ThrowInvalidCast<DateTime>(ydbValue)
        };
    }

    public TimeSpan GetInterval(int ordinal)
    {
        return GetFieldYdbValue(ordinal).GetInterval();
    }

    public override decimal GetDecimal(int ordinal)
    {
        return GetFieldYdbValue(ordinal).GetDecimal();
    }

    public override double GetDouble(int ordinal)
    {
        var ydbValue = GetFieldYdbValue(ordinal);

        return ydbValue.TypeId switch
        {
            YdbTypeId.Float => ydbValue.GetFloat(),
            YdbTypeId.Double => ydbValue.GetDouble(),
            _ => ThrowHelper.ThrowInvalidCast<double>(ydbValue)
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

        var systemType = YdbValue.GetYdbTypeId(type) switch
        {
            YdbTypeId.Timestamp or YdbTypeId.Datetime or YdbTypeId.Date => typeof(DateTime),
            YdbTypeId.Bool => typeof(bool),
            YdbTypeId.Int8 => typeof(sbyte),
            YdbTypeId.Uint8 => typeof(byte),
            YdbTypeId.Int16 => typeof(short),
            YdbTypeId.Uint16 => typeof(ushort),
            YdbTypeId.Int32 => typeof(int),
            YdbTypeId.Uint32 => typeof(uint),
            YdbTypeId.Int64 => typeof(long),
            YdbTypeId.Uint64 => typeof(ulong),
            YdbTypeId.Float => typeof(float),
            YdbTypeId.Double => typeof(double),
            YdbTypeId.Interval => typeof(TimeSpan),
            YdbTypeId.Utf8 or YdbTypeId.JsonDocument or YdbTypeId.Json or YdbTypeId.Yson =>
                typeof(string),
            YdbTypeId.String => typeof(byte[]),
            YdbTypeId.DecimalType => typeof(decimal),
            YdbTypeId.Uuid => typeof(Guid),
            _ => throw new YdbException($"Unsupported ydb type {type}")
        };

        return systemType;
    }

    public override float GetFloat(int ordinal)
    {
        return GetFieldYdbValue(ordinal).GetFloat();
    }

    public override Guid GetGuid(int ordinal)
    {
        return GetFieldYdbValue(ordinal).GetUuid();
    }

    public override short GetInt16(int ordinal)
    {
        var ydbValue = GetFieldYdbValue(ordinal);

        return ydbValue.TypeId switch
        {
            YdbTypeId.Int8 => ydbValue.GetInt8(),
            YdbTypeId.Int16 => ydbValue.GetInt16(),
            YdbTypeId.Uint8 => ydbValue.GetUint8(),
            _ => ThrowHelper.ThrowInvalidCast<short>(ydbValue)
        };
    }

    public ushort GetUint16(int ordinal)
    {
        var ydbValue = GetFieldYdbValue(ordinal);

        return ydbValue.TypeId switch
        {
            YdbTypeId.Uint8 => ydbValue.GetUint8(),
            YdbTypeId.Uint16 => ydbValue.GetUint16(),
            _ => ThrowHelper.ThrowInvalidCast<ushort>(ydbValue)
        };
    }

    public override int GetInt32(int ordinal)
    {
        var ydbValue = GetFieldYdbValue(ordinal);

        return ydbValue.TypeId switch
        {
            YdbTypeId.Int32 => ydbValue.GetInt32(),
            YdbTypeId.Int8 => ydbValue.GetInt8(),
            YdbTypeId.Int16 => ydbValue.GetInt16(),
            YdbTypeId.Uint8 => ydbValue.GetUint8(),
            YdbTypeId.Uint16 => ydbValue.GetUint16(),
            _ => ThrowHelper.ThrowInvalidCast<int>(ydbValue)
        };
    }

    public uint GetUint32(int ordinal)
    {
        var ydbValue = GetFieldYdbValue(ordinal);

        return ydbValue.TypeId switch
        {
            YdbTypeId.Uint8 => ydbValue.GetUint8(),
            YdbTypeId.Uint16 => ydbValue.GetUint16(),
            YdbTypeId.Uint32 => ydbValue.GetUint32(),
            _ => ThrowHelper.ThrowInvalidCast<uint>(ydbValue)
        };
    }

    public override long GetInt64(int ordinal)
    {
        var ydbValue = GetFieldYdbValue(ordinal);

        return ydbValue.TypeId switch
        {
            YdbTypeId.Int64 => ydbValue.GetInt64(),
            YdbTypeId.Int32 => ydbValue.GetInt32(),
            YdbTypeId.Int8 => ydbValue.GetInt8(),
            YdbTypeId.Int16 => ydbValue.GetInt16(),
            YdbTypeId.Uint8 => ydbValue.GetUint8(),
            YdbTypeId.Uint16 => ydbValue.GetUint16(),
            YdbTypeId.Uint32 => ydbValue.GetUint32(),
            _ => ThrowHelper.ThrowInvalidCast<long>(ydbValue)
        };
    }

    public ulong GetUint64(int ordinal)
    {
        var ydbValue = GetFieldYdbValue(ordinal);

        return ydbValue.TypeId switch
        {
            YdbTypeId.Uint64 => ydbValue.GetUint64(),
            YdbTypeId.Uint8 => ydbValue.GetUint8(),
            YdbTypeId.Uint16 => ydbValue.GetUint16(),
            YdbTypeId.Uint32 => ydbValue.GetUint32(),
            _ => ThrowHelper.ThrowInvalidCast<ulong>(ydbValue)
        };
    }

    public override string GetName(int ordinal)
    {
        return ReaderMetadata.GetColumn(ordinal).Name;
    }

    public override int GetOrdinal(string name)
    {
        if (ReaderMetadata.ColumnNameToOrdinal.TryGetValue(name, out var ordinal))
        {
            return ordinal;
        }

        throw new IndexOutOfRangeException($"Field not found in row: {name}");
    }

    public override string GetString(int ordinal)
    {
        return GetFieldYdbValue(ordinal).GetUtf8();
    }

    public override TextReader GetTextReader(int ordinal)
    {
        return new StringReader(GetString(ordinal));
    }

    public string GetJson(int ordinal)
    {
        return GetFieldYdbValue(ordinal).GetJson();
    }

    public string GetJsonDocument(int ordinal)
    {
        return GetFieldYdbValue(ordinal).GetJsonDocument();
    }

    public override object GetValue(int ordinal)
    {
        var ydbValue = CurrentRow[ordinal];

        // ReSharper disable once ConvertIfStatementToSwitchStatement
        if (ydbValue.TypeId == YdbTypeId.Null)
        {
            return DBNull.Value;
        }

        // ReSharper disable once InvertIf
        if (ydbValue.TypeId == YdbTypeId.OptionalType)
        {
            if (ydbValue.GetOptional() == null)
            {
                return DBNull.Value;
            }

            ydbValue = ydbValue.GetOptional()!;
        }

        return ydbValue.TypeId switch
        {
            YdbTypeId.Timestamp or YdbTypeId.Datetime or YdbTypeId.Date => GetDateTime(ordinal),
            YdbTypeId.Bool => ydbValue.GetBool(),
            YdbTypeId.Int8 => ydbValue.GetInt8(),
            YdbTypeId.Uint8 => ydbValue.GetUint8(),
            YdbTypeId.Int16 => ydbValue.GetInt16(),
            YdbTypeId.Uint16 => ydbValue.GetUint16(),
            YdbTypeId.Int32 => ydbValue.GetInt32(),
            YdbTypeId.Uint32 => ydbValue.GetUint32(),
            YdbTypeId.Int64 => ydbValue.GetInt64(),
            YdbTypeId.Uint64 => ydbValue.GetUint64(),
            YdbTypeId.Float => ydbValue.GetFloat(),
            YdbTypeId.Double => ydbValue.GetDouble(),
            YdbTypeId.Interval => ydbValue.GetInterval(),
            YdbTypeId.Utf8 => ydbValue.GetUtf8(),
            YdbTypeId.Json => ydbValue.GetJson(),
            YdbTypeId.JsonDocument => ydbValue.GetJsonDocument(),
            YdbTypeId.Yson => ydbValue.GetYson(),
            YdbTypeId.String => ydbValue.GetString(),
            YdbTypeId.DecimalType => ydbValue.GetDecimal(),
            YdbTypeId.Uuid => ydbValue.GetUuid(),
            _ => throw new YdbException($"Unsupported ydb type {ydbValue.TypeId}")
        };
    }

    public override int GetValues(object[] values)
    {
        ArgumentNullException.ThrowIfNull(values);
        if (FieldCount == 0)
        {
            throw new InvalidOperationException(" No resultset is currently being traversed");
        }

        var count = Math.Min(FieldCount, values.Length);
        for (var i = 0; i < count; i++)
            values[i] = GetValue(i);
        return count;
    }

    public override bool IsDBNull(int ordinal)
    {
        return CurrentRow[ordinal].TypeId == YdbTypeId.Null ||
               (CurrentRow[ordinal].TypeId == YdbTypeId.OptionalType && CurrentRow[ordinal].GetOptional() == null);
    }

    public override int FieldCount => ReaderMetadata.FieldCount;
    public override object this[int ordinal] => GetValue(ordinal);
    public override object this[string name] => GetValue(GetOrdinal(name));
    public override int RecordsAffected => -1;
    public override bool HasRows => ReaderMetadata.RowsCount > 0;
    public override bool IsClosed => ReaderState == State.Close;

    public override bool NextResult()
    {
        return NextResultAsync().GetAwaiter().GetResult();
    }

    public override bool Read()
    {
        return ReadAsync().GetAwaiter().GetResult();
    }

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
                while ((state = await NextExecPart()) == State.ReadResultSet)
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

        while ((ReaderState = await NextExecPart()) == State.ReadResultSet) // reset _currentRowIndex
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

        var isConsumed = ReaderState == State.IsConsumed || !await ReadAsync() && ReaderState == State.IsConsumed;
        ReaderMetadata = CloseMetadata.Instance;
        ReaderState = State.Close;

        if (isConsumed)
        {
            return;
        }

        _onNotSuccessStatus(new Status(StatusCode.SessionBusy));
        await _stream.DisposeAsync();

        if (_ydbTransaction != null)
        {
            _ydbTransaction.Failed = true;

            throw new YdbException("YdbDataReader was closed during transaction execution. Transaction is broken!");
        }
    }

    public override void Close()
    {
        CloseAsync().GetAwaiter().GetResult();
    }

    private YdbValue GetFieldYdbValue(int ordinal)
    {
        var ydbValue = CurrentRow[ordinal];

        return ydbValue.TypeId == YdbTypeId.OptionalType
            ? ydbValue.GetOptional() ?? throw new InvalidCastException("Field is null.")
            : ydbValue;
    }

    private async ValueTask<State> NextExecPart()
    {
        try
        {
            _currentRowIndex = -1;

            if (!await _stream.MoveNextAsync())
            {
                return State.IsConsumed;
            }

            var part = _stream.Current;

            _issueMessagesInStream.AddRange(part.Issues);

            if (part.Status != StatusIds.Types.StatusCode.Success)
            {
                OnFailReadStream();

                while (await _stream.MoveNextAsync())
                {
                    _issueMessagesInStream.AddRange(_stream.Current.Issues);
                }

                var status = Status.FromProto(part.Status, _issueMessagesInStream);

                _onNotSuccessStatus(status);

                throw new YdbException(status);
            }

            _currentResultSet = part.ResultSet?.FromProto();
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
        catch (Driver.TransportException e)
        {
            OnFailReadStream();

            _onNotSuccessStatus(e.Status);

            throw new YdbException(e.Status);
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

    public override async ValueTask DisposeAsync()
    {
        await CloseAsync();
    }

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

        public Value.ResultSet.Column GetColumn(int ordinal)
        {
            throw new InvalidOperationException("No resultset is currently being traversed");
        }
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

        public Value.ResultSet.Column GetColumn(int ordinal)
        {
            throw new InvalidOperationException("The reader is closed");
        }
    }

    private class Metadata : IMetadata
    {
        private IReadOnlyList<Value.ResultSet.Column> Columns { get; }

        public IReadOnlyDictionary<string, int> ColumnNameToOrdinal { get; }
        public int FieldCount { get; }
        public int RowsCount { get; }

        public Metadata(Value.ResultSet resultSet)
        {
            ColumnNameToOrdinal = resultSet.ColumnNameToOrdinal;
            Columns = resultSet.Columns;
            RowsCount = resultSet.Rows.Count;
            FieldCount = resultSet.Columns.Count;
        }

        public Value.ResultSet.Column GetColumn(int ordinal)
        {
            if (ordinal < 0 || ordinal >= FieldCount)
            {
                ThrowHelper.ThrowIndexOutOfRangeException(FieldCount);
            }

            return Columns[ordinal];
        }
    }
}
