namespace Ydb.Sdk.Services.Topic.Options;

public class WriterOptions
{
    private List<WriterOption> options;

    private WriterOptions(List<WriterOption> options) => this.options = options;

    public Builder ToBuilder() => new(options);

    public class Builder
    {
        private readonly List<WriterOption> options = new();

        internal Builder()
        {
        }

        internal Builder(List<WriterOption> options) => this.options = options;

        public WriterOptions Build()
        {
            return new WriterOptions(options);
        }
    }
}
