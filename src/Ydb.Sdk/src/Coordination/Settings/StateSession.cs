namespace Ydb.Sdk.Coordination.Settings;

public enum StateSession
{
    Initial,

    Connecting,

    Connected,
    
    Recovery,

    Reconnecting,

    Reconnected,

    Closed,

    Expired
}
