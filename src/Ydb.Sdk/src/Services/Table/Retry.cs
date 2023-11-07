using Ydb.Sdk.Client;

namespace Ydb.Sdk.Services.Table;

public partial class TableClient
{
    public async Task<IResponse> SessionExec(
        Func<Session, Task<IResponse>> operationFunc,
        RetrySettings? retrySettings = null)
    {
        if (_sessionPool is not SessionPool sessionPool)
        {
            throw new InvalidCastException(
                $"Unexpected cast error: {nameof(_sessionPool)} is not object of type {typeof(SessionPool).FullName}");
        }
        
        return await sessionPool.ExecOnSession(operationFunc, retrySettings);
    }
}
