namespace Ydb.Sdk.Ado.Retry.Delay;

public interface IStatusDelayProfile
{
    TimeSpan? GetDelay(StatusCode code, int attempt);
}
