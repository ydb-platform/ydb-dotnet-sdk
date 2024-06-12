using Ydb.Sdk.Services.Topic.Internal.Options;

namespace Ydb.Sdk.Services.Topic.Options.Reader;

internal class ReaderOption: IOption<ReaderConfig>
{
    private readonly Action<ReaderConfig> apply;

    public ReaderOption(Action<ReaderConfig> apply)
    {
        this.apply = apply;
    }

    public void Apply(ReaderConfig request)
    {
        apply(request);
    }
}