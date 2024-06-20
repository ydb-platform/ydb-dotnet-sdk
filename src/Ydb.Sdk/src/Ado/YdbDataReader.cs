using System.Collections;
using System.Data.Common;
using Ydb.Query;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado;

public sealed class YdbDataReader : DbDataReader
{
    private readonly IAsyncEnumerator<ExecuteQueryResponsePart> _stream;

    private int _currentRowIndex = -1;
    private long _resultSetIndex = -1; // not fetched result set
    private Value.ResultSet _currentResultSet = null!;

    private enum State
    {
        NotStarted,
        NewResultState,
        ReadResultState,
        Closed
    }

    private State ReaderState { get; set; }

    private Value.ResultSet CurrentResultSet => this switch
    {
        { ReaderState: State.ReadResultState, _currentRowIndex: >= 0 } => _currentResultSet,
        { ReaderState: State.Closed } => throw new InvalidOperationException("The reader is closed"),
        _ => throw new InvalidOperationException("Invalid attempt to read when no data is present")
    };

    private Value.ResultSet.Row CurrentRow => CurrentResultSet.Rows[_currentRowIndex];
    private int RowsCount => CurrentResultSet.Rows.Count;

    internal YdbDataReader(IAsyncEnumerator<ExecuteQueryResponsePart> resultSetStream)
    {
        ReaderState = State.NotStarted;
        _stream = resultSetStream;
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

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        throw new NotImplementedException();
    }

    public override char GetChar(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        throw new NotImplementedException();
    }

    public override string GetDataTypeName(int ordinal)
    {
        var ydbValue = GetFieldYdbValue(ordinal);

        // ReSharper disable once SwitchExpressionHandlesSomeKnownEnumValuesWithExceptionInDefault
        return ydbValue.TypeId switch
        {
            YdbTypeId.Timestamp => "Timestamp",
            YdbTypeId.Datetime => "Datetime",
            YdbTypeId.Date => "Date",
            _ => throw new InvalidCastException($"YdbValue: {ydbValue} can't be cast to Datetime type")
        };
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
        return GetFieldTypeAndValue(ordinal).Type;
    }

    public override float GetFloat(int ordinal)
    {
        return GetFieldYdbValue(ordinal).GetFloat();
    }

    public override Guid GetGuid(int ordinal)
    {
        throw new NotImplementedException();
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
        return CurrentResultSet.Columns[ordinal].Name;
    }

    public override int GetOrdinal(string name)
    {
        return CurrentResultSet.GetOrdinal(name);
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
        return GetFieldTypeAndValue(ordinal).Value;
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
        return CurrentRow[ordinal].TypeId == YdbTypeId.OptionalType && CurrentRow[ordinal].GetOptional() == null;
    }

    public override int FieldCount => CurrentRow.ColumnCount;

    public override object this[int ordinal] => GetValue(ordinal);
    public override object this[string name] => GetValue(GetOrdinal(name));

    public override int RecordsAffected => 0;
    public override bool HasRows => RowsCount > 0;
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
            State.NewResultState => State.ReadResultState,
            State.NotStarted or State.ReadResultState => await new Func<Task<State>>(async () =>
            {
                State state;
                while ((state = await NextResultSet()) == State.ReadResultState)
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
        var nextResult = ReaderState != State.NotStarted || await NextResultAsync(cancellationToken);

        if (!nextResult || ReaderState == State.Closed)
        {
            return false;
        }

        return ++_currentRowIndex < RowsCount ||
               ((ReaderState = await NextResultSet()) == State.ReadResultState && ++_currentRowIndex < RowsCount);
    }

    public override int Depth => 0;

    public override IEnumerator GetEnumerator()
    {
        throw new NotImplementedException();
    }

    public override async Task CloseAsync()
    {
        ReaderState = State.Closed;

        await _stream.DisposeAsync();
    }

    public override void Close()
    {
        CloseAsync().GetAwaiter().GetResult();
    }

    private YdbValue GetFieldYdbValue(int ordinal)
    {
        if (0 > ordinal || ordinal >= FieldCount)
        {
            throw new IndexOutOfRangeException("Ordinal must be between 0 and " + (FieldCount - 1));
        }

        var ydbValue = CurrentRow[ordinal];

        return ydbValue.TypeId == YdbTypeId.OptionalType
            ? ydbValue.GetOptional() ?? throw new InvalidCastException("Field is null.")
            : ydbValue;
    }

    private (System.Type Type, object Value) GetFieldTypeAndValue(int ordinal)
    {
        var ydbValue = CurrentRow[ordinal];

        // ReSharper disable once InvertIf
        if (ydbValue.TypeId == YdbTypeId.OptionalType)
        {
            if (ydbValue.GetOptional() == null)
            {
                return (typeof(DBNull), DBNull.Value);
            }

            ydbValue = ydbValue.GetOptional()!;
        }

        return ydbValue.TypeId switch
        {
            YdbTypeId.Timestamp or YdbTypeId.Datetime or YdbTypeId.Date => (typeof(string), GetDateTime(ordinal)),
            YdbTypeId.Bool => (typeof(bool), ydbValue.GetBool()),
            YdbTypeId.Int8 => (typeof(sbyte), ydbValue.GetInt8()),
            YdbTypeId.Uint8 => (typeof(byte), ydbValue.GetUint8()),
            YdbTypeId.Int16 => (typeof(short), ydbValue.GetInt16()),
            YdbTypeId.Uint16 => (typeof(ushort), ydbValue.GetUint16()),
            YdbTypeId.Int32 => (typeof(int), ydbValue.GetInt32()),
            YdbTypeId.Uint32 => (typeof(uint), ydbValue.GetUint32()),
            YdbTypeId.Int64 => (typeof(long), ydbValue.GetInt64()),
            YdbTypeId.Uint64 => (typeof(ulong), ydbValue.GetUint64()),
            YdbTypeId.Float => (typeof(float), ydbValue.GetFloat()),
            YdbTypeId.Double => (typeof(double), ydbValue.GetDouble()),
            YdbTypeId.Interval => (typeof(TimeSpan), ydbValue.GetInterval()),
            YdbTypeId.Utf8 or YdbTypeId.JsonDocument or YdbTypeId.Json or YdbTypeId.Yson =>
                (typeof(string), GetString(ordinal)),
            YdbTypeId.String => (typeof(byte[]), ydbValue.GetString()),
            YdbTypeId.DecimalType => (typeof(decimal), ydbValue.GetDecimal()),
            _ => throw new YdbAdoException($"Unsupported ydb type {ydbValue.TypeId}")
        };
    }

    private async Task<State> NextResultSet()
    {
        _currentRowIndex = -1;
        try
        {
            if (!await _stream.MoveNextAsync())
            {
                return State.Closed;
            }

            var currentExecPart = _stream.Current;

            Status.FromProto(currentExecPart.Status, currentExecPart.Issues).EnsureSuccess();

            _currentResultSet = currentExecPart.ResultSet?.FromProto() ?? Value.ResultSet.Empty;

            if (currentExecPart.ResultSetIndex <= _resultSetIndex)
            {
                return State.ReadResultState;
            }

            _resultSetIndex = currentExecPart.ResultSetIndex;

            return State.NewResultState;
        }
        catch (Exception e)
        {
            throw new YdbAdoException("Unable to read data from the transport connection", e);
        }
    }
}
