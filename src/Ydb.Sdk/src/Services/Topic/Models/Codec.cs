namespace Ydb.Sdk.Services.Topic.Models;

public enum Codec
{
    Raw = GrpcWrappers.Topic.Codecs.Codec.Raw,
    Gzip = GrpcWrappers.Topic.Codecs.Codec.Gzip,
    Lzop = GrpcWrappers.Topic.Codecs.Codec.Lzop,
    Zstd = GrpcWrappers.Topic.Codecs.Codec.Zstd
}
