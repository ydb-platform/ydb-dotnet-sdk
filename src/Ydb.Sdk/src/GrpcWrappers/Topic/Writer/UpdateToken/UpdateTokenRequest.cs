using Ydb.Topic;

namespace Ydb.Sdk.GrpcWrappers.Topic.Writer.UpdateToken;

internal class UpdateTokenRequest
{
    public string Token { get; set; } = null!;

    public StreamWriteMessage.Types.FromClient ToClientRequest()
    {
        return new StreamWriteMessage.Types.FromClient
        {
            UpdateTokenRequest = ToProto()
        };
    }

    public Ydb.Topic.UpdateTokenRequest ToProto() => new() {Token = Token};
}
