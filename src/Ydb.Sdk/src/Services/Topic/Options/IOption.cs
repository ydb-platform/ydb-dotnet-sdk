namespace Ydb.Sdk.Services.Topic.Internal.Options;

public interface IOption<in TRequest>
{
    public void Apply(TRequest request);
}