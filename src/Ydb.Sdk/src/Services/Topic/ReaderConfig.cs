namespace Ydb.Sdk.Services.Topic;

public class ReaderConfig
{
    public string Consumer { get; set; } = null!;
    public RetrySettings RetrySettings { get; set; } = new();
}
