namespace Ydb.Sdk.Services.Topic.Options;

internal class WriterOption: IOption<WriterConfig>
{
    private readonly Action<WriterConfig> _apply;

    public WriterOption(Action<WriterConfig> apply)
    {
        _apply = apply;
    }

    public void Apply(WriterConfig request)
    {
        _apply(request);
    }
}
