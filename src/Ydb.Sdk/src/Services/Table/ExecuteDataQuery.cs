using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Ydb.Sdk.Client;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Table
{
    public class ExecuteDataQuerySettings : OperationRequestSettings
    {
        public bool KeepInQueryCache { get; } = true;
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
            
            internal static ResultData FromProto(Ydb.Table.ExecuteQueryResult resultProto)
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

            var request = new Ydb.Table.ExecuteDataQueryRequest
            {
                OperationParams = MakeOperationParams(settings),
                SessionId = Id,
                TxControl = txControl.ToProto(),
                Query = new Ydb.Table.Query
                {
                    YqlText = query
                },
                QueryCachePolicy = new Ydb.Table.QueryCachePolicy
                {
                    KeepInCache = settings.KeepInQueryCache
                },
            };

            request.Parameters.Add(parameters.ToDictionary(p => p.Key, p => p.Value.GetProto()));

            try
            {
                var response = await UnaryCall(
                    method: Ydb.Table.V1.TableService.ExecuteDataQueryMethod,
                    request: request,
                    settings: settings);

                Ydb.Table.ExecuteQueryResult? resultProto;
                var status = UnpackOperation(response.Data.Operation, out resultProto);
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
}
