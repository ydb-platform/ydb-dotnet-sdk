using Ydb.Sdk.Services.Topic.Internal.Options;
using Ydb.Sdk.Services.Topic.Models.Reader;

namespace Ydb.Sdk.Services.Topic.Options;

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