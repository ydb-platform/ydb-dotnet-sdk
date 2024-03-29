﻿using Ydb.Operation.V1;
using Ydb.Operations;
using Ydb.Sdk.Client;

namespace Ydb.Sdk.Services.Operations;

public partial class OperationsClient
{
    public async Task<ClientOperation> GetOperation(string id, RequestSettings? settings = null)
    {
        settings ??= new RequestSettings();

        var request = new GetOperationRequest
        {
            Id = id
        };

        try
        {
            var response = await _driver.UnaryCall(
                method: OperationService.GetOperationMethod,
                request: request,
                settings: settings);

            return ClientOperation.FromProto(response.Data.Operation);
        }
        catch (Driver.TransportException e)
        {
            return new ClientOperation(e.Status);
        }
    }
}
