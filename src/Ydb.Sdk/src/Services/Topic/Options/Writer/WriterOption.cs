using Ydb.Sdk.Services.Topic.Internal.Options;

namespace Ydb.Sdk.Services.Topic.Options;

internal class WriterOption: IOption<WriterConfig>
{
    private readonly Action<WriterConfig> apply;

    public WriterOption(Action<WriterConfig> apply)
    {
        this.apply = apply;
    }

    public void Apply(WriterConfig request)
    {
        apply(request);
    }
}
