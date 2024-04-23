using Ydb.Sdk.Client;
using Ydb.Table;
using Ydb.Table.V1;

namespace Ydb.Sdk.Services.Table;

public class RollbackTransactionSettings : OperationRequestSettings
{
}

public class RollbackTransactionResponse : ResponseBase
{
    internal RollbackTransactionResponse(Status status) : base(status)
    {
    }
}

public partial class Session
{
    public async Task<RollbackTransactionResponse> RollbackTransaction(
        string txId,
        RollbackTransactionSettings? settings = null)
    {
        CheckSession();
        settings ??= new RollbackTransactionSettings();

        var request = new RollbackTransactionRequest
        {
            TxId = txId,
            SessionId = Id,
            OperationParams = MakeOperationParams(settings),
        };

        try
        {
            var response = await UnaryCall(TableService.RollbackTransactionMethod, request, settings);

            var status = UnpackOperation(response.Data.Operation, out ExecuteQueryResult? resultProto);
            OnResponseStatus(status);

            return new RollbackTransactionResponse(status);
        }
        catch (Driver.TransportException e)
        {
            return new RollbackTransactionResponse(e.Status);
        }
    }
}
