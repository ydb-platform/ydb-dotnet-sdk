using Ydb.Sdk.GrpcWrappers.Topic.Writer.Write;

namespace Ydb.Sdk.Services.Topic.Models.Writer;

public enum WriteResultStatus
{
    Unknown = WriteStatusType.Unknown,
    Written = WriteStatusType.Written,
    Skipped = WriteStatusType.Skipped,
}
