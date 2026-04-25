namespace Ydb.Sdk.Coordination.Settings;

public enum StateSession
{
    Initial,

    Connecting,

    Connected,

    Reconnecting,

    Reconnected,

    Closed,

    Expired
}
