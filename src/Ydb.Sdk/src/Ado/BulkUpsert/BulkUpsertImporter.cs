using Google.Protobuf.Collections;
using Ydb.Sdk.Ado.Internal;
using Ydb.Sdk.Value;
using Ydb.Table;
using Ydb.Table.V1;

namespace Ydb.Sdk.Ado.BulkUpsert;

public sealed class BulkUpsertImporter : IBulkUpsertImporter
{
    private readonly IDriver _driver;
    private readonly string _tablePath;
    private readonly IReadOnlyList<string> _columns;
    private readonly int _maxBytes;
    private readonly RepeatedField<Ydb.Value> _rows = new();
    private readonly CancellationToken _cancellationToken;
    private StructType? _structType;
    private int _currentBytes;

    internal BulkUpsertImporter(
        IDriver driver,
        string tableName,
        IReadOnlyList<string> columns,
        int maxBytes,
        CancellationToken cancellationToken = default)
    {
        _driver = driver;
        _tablePath = tableName;
        _columns = columns;
        _maxBytes = maxBytes / 2;
        _cancellationToken = cancellationToken;
    }

    public async ValueTask AddRowAsync(object?[] values)
    {
        if (values.Length != _columns.Count)
            throw new ArgumentException("Values count must match columns count", nameof(values));

        var ydbValues = values.Select(v =>
            v as YdbValue ?? (v is YdbParameter param ? param.YdbValue : new YdbParameter { Value = v }.YdbValue)
        ).ToArray();

        var protoStruct = new Ydb.Value();
        foreach (var value in ydbValues)
            protoStruct.Items.Add(value.GetProto().Value);

        var rowSize = protoStruct.CalculateSize();

        if (_currentBytes + rowSize > _maxBytes && _rows.Count > 0)
        {
            await FlushAsync(_cancellationToken);
        }

        _rows.Add(protoStruct);
        _currentBytes += rowSize;

        _structType ??= new StructType
        {
            Members =
            {
                _columns.Select((col, i) => new StructMember
                {
                    Name = col,
                    Type = ydbValues[i].GetProto().Type
                })
            }
        };
    }

    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        if (_rows.Count == 0) return;
        if (_structType == null)
            throw new InvalidOperationException("structType is undefined");

        var typedValue = new TypedValue
        {
            Type = new Type { ListType = new ListType { Item = new Type { StructType = _structType } } },
            Value = new Ydb.Value { Items = { _rows } }
        };

        var req = new BulkUpsertRequest { Table = _tablePath, Rows = typedValue };

        var resp = await _driver.UnaryCall(
            TableService.BulkUpsertMethod,
            req,
            new GrpcRequestSettings { CancellationToken = cancellationToken }
        ).ConfigureAwait(false);

        var operation = resp.Operation;
        if (operation.Status.IsNotSuccess())
            throw YdbException.FromServer(operation.Status, operation.Issues);

        _rows.Clear();
        _currentBytes = 0;
    }
}
