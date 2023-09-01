using Ydb.Sdk.Client;
using Ydb.Table;
using Ydb.Table.V1;

namespace Ydb.Sdk.Table;

public class ExecuteSchemeQuerySettings : OperationRequestSettings
{
}

public class ExecuteSchemeQueryResponse : ResponseBase
{
    internal ExecuteSchemeQueryResponse(Status status)
        : base(status)
    {
    }
}

public partial class Session
{
    public async Task<ExecuteSchemeQueryResponse> ExecuteSchemeQuery(
        string query,
        ExecuteSchemeQuerySettings? settings = null)
    {
        CheckSession();
        settings ??= new ExecuteSchemeQuerySettings();

        var request = new ExecuteSchemeQueryRequest
        {
            OperationParams = MakeOperationParams(settings),
            SessionId = Id,
            YqlText = query
        };

        try
        {
            var response = await UnaryCall(
                method: TableService.ExecuteSchemeQueryMethod,
                request: request,
                settings: settings);

            var status = UnpackOperation(response.Data.Operation);
            OnResponseStatus(status);

            return new ExecuteSchemeQueryResponse(status);
        }
        catch (Driver.TransportException e)
        {
            return new ExecuteSchemeQueryResponse(e.Status);
        }
    }
}