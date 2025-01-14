using System.Diagnostics.CodeAnalysis;
using Google.Protobuf;
using Ydb.Sdk.Client;

namespace Ydb.Sdk.Services.Operations;

internal static class OperationExtensions
{
    internal static Status Unpack(this Ydb.Operations.Operation operationProto)
    {
        var operation = ClientOperation.FromProto(operationProto);
        operation.EnsureReady();

        return operation.Status;
    }

    internal static Status TryUnpack<TResult>(this Ydb.Operations.Operation operationProto, out TResult? result)
        where TResult : class, IMessage, new()
    {
        var operation = ClientOperation.FromProto(operationProto);
        operation.EnsureReady();

        var status = operation.Status;
        result = null;

        if (operation.HasResult)
        {
            result = operation.UnpackResult<TResult>();
        }

        return status;
    }
}
