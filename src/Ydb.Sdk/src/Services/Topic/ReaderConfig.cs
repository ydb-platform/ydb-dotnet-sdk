namespace Ydb.Sdk.Services.Topic;

public class ReaderConfig
{
    public string Consumer { get; set; } = null!;
    public Models.RetrySettings RetrySettings { get; set; } = new();
}
