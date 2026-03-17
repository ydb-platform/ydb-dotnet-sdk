using Ydb.Operation.V1;
using Ydb.Operations;
using Ydb.Sdk.Client;

namespace Ydb.Sdk.Services.Operations;

public partial class OperationsClient
{
    public async Task<ClientOperation> GetOperation(string id, GrpcRequestSettings? settings = null)
    {
        settings ??= new GrpcRequestSettings();

        var request = new GetOperationRequest
        {
            Id = id
        };

        var response = await _driver.UnaryCall(
            OperationService.GetOperationMethod,
            request,
            settings);

        return ClientOperation.FromProto(response.Operation);
    }
}
