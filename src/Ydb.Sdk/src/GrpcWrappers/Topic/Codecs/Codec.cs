namespace Ydb.Sdk.GrpcWrappers.Topic.Codecs;

internal enum Codec
{
    Unspecified = 0,
    Raw = Ydb.Topic.Codec.Raw,
    Gzip = Ydb.Topic.Codec.Gzip,
    Lzop = Ydb.Topic.Codec.Lzop,
    Zstd = Ydb.Topic.Codec.Zstd
}