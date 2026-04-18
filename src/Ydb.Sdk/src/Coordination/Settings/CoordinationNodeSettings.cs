using Ydb.Sdk.Coordination.Description;

namespace Ydb.Sdk.Coordination.Settings;

public class CoordinationNodeSettings : OperationSettings
{
    public NodeConfig Config { get; init; }
}
