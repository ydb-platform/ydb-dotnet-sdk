using Ydb.Sdk.Client;
using Ydb.Sdk.Value;
using Ydb.Table;
using Ydb.Table.V1;

namespace Ydb.Sdk.Services.Table;

public class ExecuteScanQuerySettings : GrpcRequestSettings
{
}

public class ExecuteScanQueryPart : ResponseWithResultBase<ExecuteScanQueryPart.ResultData>
{
    internal ExecuteScanQueryPart(Status status, ResultData? result = null)
        : base(status, result)
    {
    }

    public class ResultData
    {
        internal ResultData(Value.ResultSet? resultSetPart)
        {
            ResultSetPart = resultSetPart;
        }

        public Value.ResultSet? ResultSetPart { get; }

        internal static ResultData FromProto(ExecuteScanQueryPartialResult resultProto)
        {
            return new ResultData(resultProto.ResultSet?.FromProto());
        }
    }
}

public class ExecuteScanQueryStream : StreamResponse<ExecuteScanQueryPartialResponse, ExecuteScanQueryPart>
{
    internal ExecuteScanQueryStream(Driver.ServerStream<ExecuteScanQueryPartialResponse> iterator)
        : base(iterator)
    {
    }

    protected override ExecuteScanQueryPart MakeResponse(Status status)
    {
        return new ExecuteScanQueryPart(status);
    }

    protected override ExecuteScanQueryPart MakeResponse(ExecuteScanQueryPartialResponse protoResponse)
    {
        var status = Status.FromProto(protoResponse.Status, protoResponse.Issues);
        var result = status.IsSuccess && protoResponse.Result != null
            ? ExecuteScanQueryPart.ResultData.FromProto(protoResponse.Result)
            : null;

        return new ExecuteScanQueryPart(status, result);
    }
}

public partial class TableClient
{
    public ExecuteScanQueryStream ExecuteScanQuery(
        string query,
        IReadOnlyDictionary<string, YdbValue> parameters,
        ExecuteScanQuerySettings? settings = null)
    {
        settings ??= new ExecuteScanQuerySettings();

        var request = new ExecuteScanQueryRequest
        {
            Mode = ExecuteScanQueryRequest.Types.Mode.Exec,
            Query = new Ydb.Table.Query
            {
                YqlText = query
            }
        };

        request.Parameters.Add(parameters.ToDictionary(p => p.Key, p => p.Value.GetProto()));

        var streamIterator = _driver.ServerStreamCall(
            method: TableService.StreamExecuteScanQueryMethod,
            request: request,
            settings: settings
        );

        return new ExecuteScanQueryStream(streamIterator);
    }

    public ExecuteScanQueryStream ExecuteScanQuery(
        string query,
        ExecuteScanQuerySettings? settings = null)
    {
        return ExecuteScanQuery(query, new Dictionary<string, YdbValue>(), settings);
    }
}
