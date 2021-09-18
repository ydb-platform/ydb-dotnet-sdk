using Google.Protobuf.WellKnownTypes;

namespace Ydb.Sdk.Client
{
    public class ClientBase
    {
        protected internal Driver Driver { get; }

        protected internal ClientBase(Driver driver)
        {
            Driver = driver;
        }

        protected internal static Status UnpackOperation(Ydb.Operations.Operation operationProto)
        {
            var operation = ClientOperation.FromProto(operationProto);
            operation.EnsureReady();
            return operation.Status;
        }

        protected internal static Status UnpackOperation<TResult>(Ydb.Operations.Operation operationProto, out TResult? result)
            where TResult : class, Google.Protobuf.IMessage, new()
        {
            var operation = ClientOperation.FromProto(operationProto);
            operation.EnsureReady();

            Status status = operation.Status;

            if (!operation.HasResult)
            {
                result = null;
                return status;
            }

            result = operation.UnpackResult<TResult>();
            return status;
        }

        protected internal static Ydb.Operations.OperationParams MakeOperationParams(OperationRequestSettings settings)
        {
            var opParams = new Ydb.Operations.OperationParams();

            if (settings.OperationTimeout != null)
            {
                opParams.OperationTimeout = Duration.FromTimeSpan(settings.OperationTimeout.Value);
            }

            return opParams;
        }
    }
}
