namespace Ydb.Sdk.Services.Topic.Options;

public class DescribeOptions
{
    private List<DescribeOption> options;

    private DescribeOptions(List<DescribeOption> options) => this.options = options;

    public Builder ToBuilder() => new(options);

    public class Builder
    {
        private readonly List<DescribeOption> options = new();

        internal Builder()
        {
        }

        internal Builder(List<DescribeOption> options) => this.options = options;

        public DescribeOptions Build()
        {
            return new DescribeOptions(options);
        }
    }
}
