using Google.Protobuf.WellKnownTypes;
using System;
using System.Diagnostics;

namespace Ydb.Sdk.Client
{
    public interface IClientOperation
    {
        public string Id { get; }

        public bool IsReady { get; }

        public Status Status { get; }

        public bool HasResult { get; }

        public bool HasMetadata { get; }
    }

    public class ClientOperation : IClientOperation
    {
        private readonly Any? _result;
        private readonly Any? _metadata;

        public ClientOperation(Status status)
        {
            Id = String.Empty;
            IsReady = true;
            Status = status;
        }

        private ClientOperation(Ydb.Operations.Operation operation)
        {
            Id = operation.Id;
            IsReady = operation.Ready;
            Status = Status.FromProto(operation.Status, operation.Issues);
            _result = operation.Result;
            _metadata = operation.Metadata;
        }

        public string Id { get; }

        public bool IsReady { get; }

        public Status Status { get; }

        public bool HasResult
        {
            get { return IsReady && _result != null; }
        }

        public bool HasMetadata
        {
            get { return _metadata != null; }
        }

        public Google.Protobuf.WellKnownTypes.Any Metadata
        {
            get
            {
                if (_metadata is null)
                {
                    throw new OperationException(Id, "Operation metadata unavailable.");
                }

                return _metadata;
            }
        }

        public void EnsureReady()
        {
            if (!IsReady)
            {
                throw new OperationException(Id, "Operation not ready.");
            }
        }

        public TResult UnpackResult<TResult>()
            where TResult : Google.Protobuf.IMessage, new()
        {
            EnsureReady();

            if (!HasResult)
            {
                throw new OperationException(Id, "Operation result unavailable.");
            }

            Debug.Assert(_result != null);
            return _result.Unpack<TResult>();
        }

        internal static ClientOperation FromProto(Ydb.Operations.Operation operationProto)
        {
            return new ClientOperation(operationProto);
        }
    }

    public class OperationException : Exception
    {
        public OperationException(string operationId, string message)
            : base($"Operation {operationId}: {message}")
        {
            OperationId = operationId;
        }

        public string OperationId { get; }
    }

    public class OperationNotReadyException : Exception
    {
        public OperationNotReadyException(string operationId)
        {
            OperationId = operationId;
        }

        public string OperationId { get; }
    }
}
