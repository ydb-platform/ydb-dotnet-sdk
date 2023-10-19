using Ydb.Sdk.Client;
using Ydb.Sdk.Value;
using Ydb.Table;
using Ydb.Table.V1;

namespace Ydb.Sdk.Services.Table;

public class ExecuteDataQuerySettings : OperationRequestSettings
{
    public bool KeepInQueryCache { get; set; } = true;
}

public class ExecuteDataQueryResponse : ResponseWithResultBase<ExecuteDataQueryResponse.ResultData>
{
    internal ExecuteDataQueryResponse(
        Status status,
        TransactionState txState,
        Transaction? tx = null,
        ResultData? result = null)
        : base(status, result)
    {
        Tx = tx;
        TxState = txState;
    }

    public TransactionState TxState { get; }

    public Transaction? Tx { get; }

    public class ResultData
    {
        internal ResultData(IReadOnlyList<Value.ResultSet> resultSets)
        {
            ResultSets = resultSets;
        }

        public IReadOnlyList<Value.ResultSet> ResultSets { get; }

        internal static ResultData FromProto(ExecuteQueryResult resultProto)
        {
            var resultSets = resultProto.ResultSets
                .Select(r => Value.ResultSet.FromProto(r))
                .ToList();

            return new ResultData(
                resultSets: resultSets
            );
        }
    }
}

public partial class Session
{
    public async Task<ExecuteDataQueryResponse> ExecuteDataQuery(
        string query,
        TxControl txControl,
        IReadOnlyDictionary<string, YdbValue> parameters,
        ExecuteDataQuerySettings? settings = null)
    {
        CheckSession();
        settings ??= new ExecuteDataQuerySettings();

        var request = new ExecuteDataQueryRequest
        {
            OperationParams = MakeOperationParams(settings),
            SessionId = Id,
            TxControl = txControl.ToProto(),
            Query = new Query
            {
                YqlText = query
            },
            QueryCachePolicy = new QueryCachePolicy
            {
                KeepInCache = settings.KeepInQueryCache
            }
        };

        request.Parameters.Add(parameters.ToDictionary(p => p.Key, p => p.Value.GetProto()));

        try
        {
            var response = await UnaryCall(
                method: TableService.ExecuteDataQueryMethod,
                request: request,
                settings: settings);

            var status = UnpackOperation(response.Data.Operation, out ExecuteQueryResult? resultProto);
            OnResponseStatus(status);

            var txState = TransactionState.Unknown;
            Transaction? tx = null;
            if (resultProto != null && resultProto.TxMeta != null)
            {
                txState = resultProto.TxMeta.Id.Length > 0
                    ? TransactionState.Active
                    : TransactionState.Void;

                tx = Transaction.FromProto(resultProto.TxMeta);
            }

            ExecuteDataQueryResponse.ResultData? result = null;
            if (status.IsSuccess && resultProto != null)
            {
                result = ExecuteDataQueryResponse.ResultData.FromProto(resultProto);
            }

            return new ExecuteDataQueryResponse(status, txState, tx, result);
        }
        catch (Driver.TransportException e)
        {
            return new ExecuteDataQueryResponse(e.Status, TransactionState.Unknown);
        }
    }

    public async Task<ExecuteDataQueryResponse> ExecuteDataQuery(
        string query,
        TxControl txControl,
        ExecuteDataQuerySettings? settings = null)
    {
        return await ExecuteDataQuery(query, txControl, new Dictionary<string, YdbValue>(), settings);
    }
}