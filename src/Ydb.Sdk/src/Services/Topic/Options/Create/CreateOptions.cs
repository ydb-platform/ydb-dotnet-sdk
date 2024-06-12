namespace Ydb.Sdk.Services.Topic.Options;

public class CreateOptions
{
    private List<CreateOption> options;

    private CreateOptions(List<CreateOption> options) => this.options = options;

    public Builder ToBuilder() => new(options);

    public class Builder
    {
        private readonly List<CreateOption> options = new();

        internal Builder()
        {
        }

        internal Builder(List<CreateOption> options) => this.options = options;

        public CreateOptions Build()
        {
            return new CreateOptions(options);
        }
    }
}
