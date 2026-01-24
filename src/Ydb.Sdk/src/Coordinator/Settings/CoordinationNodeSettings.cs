using Ydb.Sdk.Coordinator.Description;

namespace Ydb.Sdk.Coordinator.Settings;

public class CoordinationNodeSettings : OperationSettings
{
    //  проблемное место, а билдер был заменен на extension, но он как будто не нужен,
    
    public NodeConfig Config { get; set; }

    public CoordinationNodeSettings(NodeConfig config)
    {
        Config = config;
    }
}
