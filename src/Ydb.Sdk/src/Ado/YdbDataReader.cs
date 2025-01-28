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

    private enum State
    {
        Initialized,
        NewResultState,
        ReadResultState,
        Closed
    }

    private interface IMetadata
    {
        IReadOnlyDictionary<string, int> ColumnNameToOrdinal { get; }

        IReadOnlyList<Value.ResultSet.Column> Columns { get; }

        int FieldCount { get; }

        int RowsCount { get; }
    }

    private State ReaderState { get; set; }
    private IMetadata ReaderMetadata { get; set; } = null!;

    private Value.ResultSet CurrentResultSet => this switch
    {
        { ReaderState: State.ReadResultState, _currentRowIndex: >= 0 } => _currentResultSet!,
        { ReaderState: State.Closed } => throw new InvalidOperationException("The reader is closed"),
        _ => throw new InvalidOperationException("No row is available")
    };

    private Value.ResultSet.Row CurrentRow => CurrentResultSet.Rows[_currentRowIndex];
    private int RowsCount => ReaderMetadata.RowsCount;

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
        if (State.Closed == await NextExecPart())
        {
            throw new YdbException("YDB server closed the stream");
        }

        ReaderState = State.Initialized;
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
            return 0;
        }

        var copyCount = Math.Min(bytes.Length - dataOffset, length);
        Array.Copy(bytes, (int)dataOffset, buffer, bufferOffset, copyCount);

        return copyCount;
    }

    public override char GetChar(int ordinal)
    {
        return GetString(ordinal)[0];
    }

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        var chars = GetString(ordinal).ToCharArray();

        CheckOffsets(dataOffset, buffer, bufferOffset, length);

        if (buffer == null)
        {
            return 0;
        }

        var copyCount = Math.Min(chars.Length - dataOffset, length);
        Array.Copy(chars, (int)dataOffset, buffer, bufferOffset, copyCount);

        return copyCount;
    }

    private static void CheckOffsets<T>(long dataOffset, T[]? buffer, int bufferOffset, int length)
    {
        if (dataOffset is < 0 or > int.MaxValue)
        {
            throw new IndexOutOfRangeException($"dataOffset must be between 0 and {int.MaxValue}");
        }

        if (buffer != null && (bufferOffset < 0 || bufferOffset >= buffer.Length))
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
        return ReaderMetadata.Columns[ordinal].Type.TypeId.ToString();
    }

    public override DateTime GetDateTime(int ordinal)
    {
        var ydbValue = GetFieldYdbValue(ordinal);

        // ReSharper disable once SwitchExpressionHandlesSomeKnownEnumValuesWithExceptionInDefault
        return ydbValue.TypeId switch
        {
            YdbTypeId.Timestamp => ydbValue.GetTimestamp(),
            YdbTypeId.Datetime => ydbValue.GetDatetime(),
            YdbTypeId.Date => ydbValue.GetDate(),
            _ => throw new InvalidCastException($"Field type {ydbValue.TypeId} can't be cast to DateTime type")
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
        return GetFieldYdbValue(ordinal).GetDouble();
    }

    public override System.Type GetFieldType(int ordinal)
    {
        var type = ReaderMetadata.Columns[ordinal].Type;

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
        throw new YdbException("Ydb does not supported Guid");
    }

    public override short GetInt16(int ordinal)
    {
        return GetFieldYdbValue(ordinal).GetInt16();
    }

    public ushort GetUint16(int ordinal)
    {
        return GetFieldYdbValue(ordinal).GetUint16();
    }

    public override int GetInt32(int ordinal)
    {
        return GetFieldYdbValue(ordinal).GetInt32();
    }

    public uint GetUint32(int ordinal)
    {
        return GetFieldYdbValue(ordinal).GetUint32();
    }

    public override long GetInt64(int ordinal)
    {
        return GetFieldYdbValue(ordinal).GetInt64();
    }

    public ulong GetUint64(int ordinal)
    {
        return GetFieldYdbValue(ordinal).GetUint64();
    }

    public override string GetName(int ordinal)
    {
        return ReaderMetadata.Columns[ordinal].Name;
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
        EnsureOrdinal(ordinal);

        var ydbValue = CurrentRow[ordinal];

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
        var count = Math.Min(FieldCount, values.Length);
        for (var i = 0; i < count; i++)
            values[i] = GetValue(i);
        return count;
    }

    public override bool IsDBNull(int ordinal)
    {
        return CurrentRow[ordinal].TypeId == YdbTypeId.Unknown ||
               (CurrentRow[ordinal].TypeId == YdbTypeId.OptionalType && CurrentRow[ordinal].GetOptional() == null);
    }

    public override int FieldCount => ReaderMetadata.FieldCount;
    public override object this[int ordinal] => GetValue(ordinal);
    public override object this[string name] => GetValue(GetOrdinal(name));
    public override int RecordsAffected => 0;
    public override bool HasRows => ReaderMetadata.RowsCount > 0;
    public override bool IsClosed => ReaderState == State.Closed;

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
        ReaderState = ReaderState switch
        {
            State.Closed => State.Closed,
            State.Initialized or State.NewResultState => State.ReadResultState,
            State.ReadResultState => await new Func<Task<State>>(async () =>
            {
                State state;
                while ((state = await NextExecPart()) == State.ReadResultState)
                {
                }

                return state == State.NewResultState ? State.ReadResultState : state;
            })(),
            _ => throw new ArgumentOutOfRangeException(ReaderState.ToString())
        };

        return ReaderState != State.Closed;
    }

    public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        var nextResult = ReaderState != State.Initialized || await NextResultAsync(cancellationToken);

        if (!nextResult || ReaderState == State.Closed)
        {
            return false;
        }

        if (++_currentRowIndex < RowsCount)
        {
            return true;
        }

        while ((ReaderState = await NextExecPart()) == State.ReadResultState) // reset _currentRowIndex
        {
            if (++_currentRowIndex < RowsCount)
            {
                return true;
            }
        }

        return false;
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
        if (ReaderState == State.Closed)
        {
            return;
        }

        ReaderState = State.Closed;
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
        EnsureOrdinal(ordinal);

        var ydbValue = CurrentRow[ordinal];

        return ydbValue.TypeId == YdbTypeId.OptionalType
            ? ydbValue.GetOptional() ?? throw new InvalidCastException("Field is null.")
            : ydbValue;
    }

    private void EnsureOrdinal(int ordinal)
    {
        if (ordinal >= FieldCount || 0 > ordinal) // get FieldCount throw InvalidOperationException if State == Closed
        {
            throw new IndexOutOfRangeException("Ordinal must be between 0 and " + (FieldCount - 1));
        }
    }

    private async Task<State> NextExecPart()
    {
        try
        {
            _currentRowIndex = -1;

            if (!await _stream.MoveNextAsync())
            {
                return State.Closed;
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
                return State.ReadResultState;
            }

            _resultSetIndex = part.ResultSetIndex;

            return State.NewResultState;
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
        ReaderState = State.Closed;
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

        public IReadOnlyList<Value.ResultSet.Column> Columns =>
            throw new InvalidOperationException("No resultset is currently being traversed");

        public int FieldCount => 0;
        public int RowsCount => 0;
    }

    private class Metadata : IMetadata
    {
        public IReadOnlyDictionary<string, int> ColumnNameToOrdinal { get; }
        public IReadOnlyList<Value.ResultSet.Column> Columns { get; }
        public int FieldCount { get; }
        public int RowsCount { get; }

        public Metadata(Value.ResultSet resultSet)
        {
            ColumnNameToOrdinal = resultSet.ColumnNameToOrdinal;
            Columns = resultSet.Columns;
            RowsCount = resultSet.Rows.Count;
            FieldCount = resultSet.Columns.Count;
        }
    }
}
