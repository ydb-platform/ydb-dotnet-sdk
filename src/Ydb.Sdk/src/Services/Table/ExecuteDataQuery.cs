﻿using Ydb.Sdk.Client;
using Ydb.Sdk.Services.Operations;
using Ydb.Sdk.Value;
using Ydb.Table;
using Ydb.Table.V1;

namespace Ydb.Sdk.Services.Table;

public class ExecuteDataQuerySettings : OperationSettings
{
    public bool KeepInQueryCache { get; set; } = true;
    public bool AllowTruncated { get; set; }
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
                .Select(Value.ResultSet.FromProto)
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
            OperationParams = settings.MakeOperationParams(),
            SessionId = Id,
            TxControl = txControl.ToProto(Logger),
            Query = new Ydb.Table.Query
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
                settings: settings
            );

            var status = response.Data.Operation.TryUnpack(out ExecuteQueryResult? resultProto);
            OnResponseStatus(status);

            var txState = TransactionState.Unknown;
            Transaction? tx = null;
            if (resultProto?.TxMeta != null)
            {
                txState = resultProto.TxMeta.Id.Length > 0
                    ? TransactionState.Active
                    : TransactionState.Void;

                tx = Transaction.FromProto(resultProto.TxMeta, Logger);
            }

            ExecuteDataQueryResponse.ResultData? result = null;
            if (status.IsSuccess && resultProto != null)
            {
                result = ExecuteDataQueryResponse.ResultData.FromProto(resultProto);
                if (!settings.AllowTruncated && result.ResultSets.Any(set => set.Truncated))
                {
                    throw new TruncateException();
                }
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

public class TruncateException : Exception
{
}
