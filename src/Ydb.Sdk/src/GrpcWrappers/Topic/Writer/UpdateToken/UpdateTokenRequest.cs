namespace Ydb.Sdk.GrpcWrappers.Topic.Writer.UpdateToken;

internal class UpdateTokenRequest
{
    public string Token { get; set; }

    public Ydb.Topic.UpdateTokenRequest ToProto() => new() {Token = Token};
}
