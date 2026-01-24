using Ydb.Sdk.Coordinator.Description;

namespace Ydb.Sdk.Coordinator.Settings;

public class CoordinationNodeSettings : OperationSettings
{
    
    //  проблемное место, конфиг приватные,а билдер был заменен на extension,
    // но у Config должен быть какое-то значение в идеале
    public NodeConfig Config { get; set; } 
    
}
