namespace Ydb.Sdk.Services.Topic.Options;

public class DropOptions
{
    private List<DropOption> options;

    private DropOptions(List<DropOption> options) => this.options = options;

    public Builder ToBuilder() => new(options);

    public class Builder
    {
        private readonly List<DropOption> options = new();

        internal Builder()
        {
        }

        internal Builder(List<DropOption> options) => this.options = options;

        public DropOptions Build()
        {
            return new DropOptions(options);
        }
    }
}
