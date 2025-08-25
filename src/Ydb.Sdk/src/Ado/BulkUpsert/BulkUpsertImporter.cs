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
    private readonly int _maxBatchByteSize;
    private readonly RepeatedField<Ydb.Value> _rows = new();
    private readonly CancellationToken _cancellationToken;
    private StructType? _structType;
    private int _currentBytes;

    internal BulkUpsertImporter(
        IDriver driver,
        string tableName,
        IReadOnlyList<string> columns,
        int maxBatchByteSize,
        CancellationToken cancellationToken = default)
    {
        _driver = driver;
        _tablePath = tableName;
        _columns = columns;
        _maxBatchByteSize = maxBatchByteSize / 2;
        _cancellationToken = cancellationToken;
    }

    /// <summary>
    /// Add a single row to the current BulkUpsert batch.
    /// </summary>
    /// <param name="values">Values in the same order as the configured <c>columns</c>.</param>
    /// <remarks>
    /// Supported per-cell types: <see cref="YdbValue"/>, <see cref="YdbParameter"/>.
    /// Other CLR values are converted via <see cref="YdbParameter"/>.
    /// Passing <see cref="YdbList"/> as a column value is not supported (tables do not accept list-typed columns).
    /// Use <c>AddListAsync(YdbList)</c> to append many rows from a list parameter.
    /// </remarks>
    /// <exception cref="ArgumentException">Thrown when the number of values differs from the number of columns.</exception>
    /// <exception cref="InvalidOperationException">Thrown when a value cannot be mapped to a YDB type.</exception>
    /// <example>
    /// <code>
    /// // columns: ["Id", "Name"]
    /// await importer.AddRowAsync(1, "Alice");
    /// await importer.AddRowAsync(2, "Bob");
    /// </code>
    /// </example>
    public async ValueTask AddRowAsync(params object[] values)
    {
        if (values.Length != _columns.Count)
            throw new ArgumentException("Values count must match columns count.", nameof(values));

        var ydbValues = values.Select(v => v switch
        {
            YdbValue ydbValue  => ydbValue.GetProto(),
            YdbParameter param => param.TypedValue,
            YdbList => throw new ArgumentException(
                "YdbList cannot be used as a column value. Use AddListAsync(YdbList) to append multiple rows.",
                nameof(values)),
            _ => new YdbParameter { Value = v }.TypedValue
        }).ToArray();

        var protoStruct = new Ydb.Value();
        foreach (var tv in ydbValues)
            protoStruct.Items.Add(tv.Value);

        var rowSize = protoStruct.CalculateSize();

        if (_currentBytes + rowSize > _maxBatchByteSize && _rows.Count > 0)
            await FlushAsync();

        _rows.Add(protoStruct);
        _currentBytes += rowSize;

        _structType ??= new StructType
        {
            Members = { _columns.Select((col, i) => new StructMember { Name = col, Type = ydbValues[i].Type }) }
        };
    }

    /// <summary>
    /// Add multiple rows from a <see cref="YdbList"/> shaped as <c>List&lt;Struct&lt;...&gt;&gt;</c>.
    /// Struct member names and order must exactly match the configured <c>columns</c>.
    /// </summary>
    /// <param name="list">Rows as <c>List&lt;Struct&lt;...&gt;&gt;</c> with the exact column names and order.</param>
    /// <exception cref="ArgumentException">
    /// Thrown when the struct column set, order, or count does not match the importer’s <c>columns</c>.
    /// </exception>
    public async ValueTask AddListAsync(YdbList list)
    {
        var tv = list.ToTypedValue();

        var incomingStruct = tv.Type.ListType.Item.StructType;

        if (incomingStruct.Members.Count != _columns.Count)
            throw new ArgumentException(
                $"The number of columns in the List<Struct> ({incomingStruct.Members.Count}) " +
                $"does not match the expected ({_columns.Count}).");

        for (var i = 0; i < _columns.Count; i++)
        {
            var expected = _columns[i];
            var actual = incomingStruct.Members[i].Name;
            if (!string.Equals(expected, actual, StringComparison.Ordinal))
                throw new ArgumentException(
                    $"Column name mismatch at position {i}: expected '{expected}', received '{actual}'.");
        }

        _structType ??= incomingStruct;

        foreach (var rowValue in tv.Value.Items)
        {
            var rowSize = rowValue.CalculateSize();

            if (_currentBytes + rowSize > _maxBatchByteSize && _rows.Count > 0)
                await FlushAsync().ConfigureAwait(false);

            _rows.Add(rowValue);
            _currentBytes += rowSize;
        }
    }

    /// <summary>
    /// Flush the current batch via BulkUpsert. No-op if the batch is empty.
    /// </summary>
    /// <remarks>
    /// Uses the collected struct schema from the first added row (or the provided list) and sends
    /// the accumulated rows in a single BulkUpsert request.
    /// </remarks>
    public async ValueTask FlushAsync()
    {
        if (_rows.Count == 0)
            return;

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
            new GrpcRequestSettings { CancellationToken = _cancellationToken }
        ).ConfigureAwait(false);

        var operation = resp.Operation;
        if (operation.Status.IsNotSuccess())
            throw YdbException.FromServer(operation.Status, operation.Issues);

        _rows.Clear();
        _currentBytes = 0;
    }
}
