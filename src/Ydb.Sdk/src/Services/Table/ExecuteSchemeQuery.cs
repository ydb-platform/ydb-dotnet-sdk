using Ydb.Sdk.Client;
using Ydb.Sdk.Services.Operations;
using Ydb.Table;
using Ydb.Table.V1;

namespace Ydb.Sdk.Services.Table;

public class ExecuteSchemeQuerySettings : OperationSettings
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
            OperationParams = settings.MakeOperationParams(),
            SessionId = Id,
            YqlText = query
        };

        var response = await UnaryCall(
            method: TableService.ExecuteSchemeQueryMethod,
            request: request,
            settings: settings
        );

        var status = response.Operation.Unpack();
        OnResponseStatus(status);

        return new ExecuteSchemeQueryResponse(status);
    }
}
