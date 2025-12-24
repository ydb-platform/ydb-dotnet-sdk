using BenchmarkDotNet.Attributes;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Ado.BulkUpsert;
using Ydb.Table;

namespace Ydb.Sdk.Ado.Benchmarks;

public class BulkUpsertImporterBenchmark
{
    private const int BatchSize = 15000;
    private readonly int _maxBatchByteSize = new YdbConnectionStringBuilder().MaxSendMessageSize;
    private IDriver _driver = null!;
    private IList<(Guid Guid, string String, int Int, double Double, DateTime DateTime)> _rows = null!;

    [GlobalSetup]
    public void Setup()
    {
        _driver = new BulkUpsertMockDriver();
        _rows = Enumerable.Range(0, BatchSize)
            .Select(i => new ValueTuple<Guid, string, int, double, DateTime>(Guid.NewGuid(),
                Guid.NewGuid() + "_" + Guid.NewGuid(), i, i * 1.324, DateTime.Now))
            .ToList();
    }

    [Benchmark]
    public async Task FlushAsync_BulkUpsertImporter()
    {
        var bulkUpsertImporter = new BulkUpsertImporter(_driver, "table_name", ["c1", "c2", "c3", "c4", "c5"],
            _maxBatchByteSize, CancellationToken.None);

        foreach (var row in _rows)
        {
            await bulkUpsertImporter.AddRowAsync(row.Guid, row.String, row.Int, row.Double, row.DateTime);
        }

        await bulkUpsertImporter.FlushAsync();
    }
}

internal class BulkUpsertMockDriver : IDriver
{
    public ValueTask DisposeAsync() => throw new NotImplementedException();

    public void Dispose() => throw new NotImplementedException();

    public Task<TResponse>
        UnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, TRequest request,
            GrpcRequestSettings settings) where TRequest : class where TResponse : class =>
        Task.FromResult((TResponse)(object)new BulkUpsertResponse
        {
            Operation = new Operations.Operation { Status = StatusIds.Types.StatusCode.Success }
        });

    public ValueTask<IServerStream<TResponse>> ServerStreamCall<TRequest,
        TResponse>(Method<TRequest, TResponse> method, TRequest request, GrpcRequestSettings settings) where
        TRequest : class
        where TResponse : class => throw new NotImplementedException();

    public ValueTask<IBidirectionalStream<TRequest,
        TResponse>> BidirectionalStreamCall<TRequest, TResponse>(Method<TRequest, TResponse> method,
        GrpcRequestSettings settings) where TRequest : class where TResponse : class => throw new
        NotImplementedException();

    public ILoggerFactory LoggerFactory => null!;

    public void RegisterOwner() => throw new NotImplementedException();

    public bool IsDisposed => false;
    public string Database => throw new NotImplementedException();
}
