namespace Ydb.Sdk.Services.Topic.Options.Reader;

public class ReaderOptions
{
    private List<ReaderOption> options;

    private ReaderOptions(List<ReaderOption> options) => this.options = options;

    public Builder ToBuilder() => new(options);

    public class Builder
    {
        private readonly List<ReaderOption> options = new();

        internal Builder()
        {
        }

        internal Builder(List<ReaderOption> options) => this.options = options;

        public ReaderOptions Build()
        {
            return new ReaderOptions(options);
        }
    }
}
