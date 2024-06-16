namespace Ydb.Sdk.Services.Topic.Options;

public interface IOption<in TRequest>
{
    public void Apply(TRequest request);
}
