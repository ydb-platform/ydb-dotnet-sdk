using Ydb.Sdk.Client;
using Ydb.Sdk.Services.Operations;
using Ydb.Table;
using Ydb.Table.V1;

namespace Ydb.Sdk.Services.Table;

public class DeleteSessionSettings : OperationSettings
{
}

public class DeleteSessionResponse : ResponseBase
{
    internal DeleteSessionResponse(Status status) : base(status)
    {
    }
}

public partial class TableClient
{
    public async Task<DeleteSessionResponse> DeleteSession(string sessionId, DeleteSessionSettings? settings = null)
    {
        settings ??= new DeleteSessionSettings();

        var request = new DeleteSessionRequest
        {
            OperationParams = settings.MakeOperationParams(),
            SessionId = sessionId
        };

        try
        {
            var response = await _driver.UnaryCall(
                method: TableService.DeleteSessionMethod,
                request: request,
                settings: settings);

            var status = response.Data.Operation.Unpack();

            return new DeleteSessionResponse(status);
        }
        catch (Driver.TransportException e)
        {
            return new DeleteSessionResponse(e.Status);
        }
    }
}
