using Ydb.Sdk.Client;

namespace Ydb.Sdk.Services.Table;

public partial class TableClient
{
    public async Task<IResponse> SessionExec(
        Func<Session, Task<IResponse>> operationFunc,
        RetrySettings? retrySettings = null)
    {
        retrySettings ??= new RetrySettings();

        IResponse response = new ClientInternalErrorResponse("SessionRetry, unexpected response value.");
        Session? session = null;
        try
        {
            for (uint attemptNumber = 0; attemptNumber < retrySettings.MaxAttempts; ++attemptNumber)
            {
                if (session is null)
                {
                    var sessionResponse = await _sessionPool.GetSession();
                    response = sessionResponse;

                    if (sessionResponse.Status.IsSuccess)
                    {
                        session = sessionResponse.Result;
                    }
                }

                if (session != null)
                {
                    var operationResponse = await operationFunc(session);
                    if (operationResponse.Status.IsSuccess)
                    {
                        return operationResponse;
                    }

                    response = operationResponse;
                }

                switch (response.Status.StatusCode)
                {
                    case StatusCode.Aborted:
                    case StatusCode.Unavailable:
                        await Task.Delay(retrySettings.FastBackoff.CalcBackoff(attemptNumber));
                        break;

                    case StatusCode.Overloaded:
                    case StatusCode.ClientResourceExhausted:
                    case StatusCode.ClientTransportResourceExhausted:
                        await Task.Delay(retrySettings.SlowBackoff.CalcBackoff(attemptNumber));
                        break;

                    case StatusCode.BadSession:
                    case StatusCode.SessionBusy:
                        if (session != null)
                        {
                            session.Dispose();
                        }

                        session = null;
                        break;

                    default:
                        return response;
                }
            }
        }
        finally
        {
            if (session != null)
            {
                session.Dispose();
            }
        }

        return response;
    }
}