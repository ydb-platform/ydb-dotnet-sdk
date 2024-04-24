using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Ydb.Operations;

namespace Ydb.Sdk.Client;

public abstract class ClientBase
{
    protected Driver Driver { get; }

    protected internal ClientBase(Driver driver)
    {
        Driver = driver;
    }

    protected static Status UnpackOperation(Operations.Operation operationProto)
    {
        var operation = ClientOperation.FromProto(operationProto);
        operation.EnsureReady();
        return operation.Status;
    }

    protected static Status UnpackOperation<TResult>(Operations.Operation operationProto,
        out TResult? result)
        where TResult : class, IMessage, new()
    {
        var operation = ClientOperation.FromProto(operationProto);
        operation.EnsureReady();

        var status = operation.Status;

        if (!operation.HasResult)
        {
            result = null;
            return status;
        }

        result = operation.UnpackResult<TResult>();
        return status;
    }

    protected static OperationParams MakeOperationParams(OperationRequestSettings settings)
    {
        var opParams = new OperationParams();

        if (settings.OperationTimeout != null)
        {
            opParams.OperationTimeout = Duration.FromTimeSpan(settings.OperationTimeout.Value);
        }

        return opParams;
    }
}
