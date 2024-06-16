using Ydb.Sdk.GrpcWrappers.Topic.Codecs;

namespace Ydb.Sdk.GrpcWrappers.Topic.Extensions;

internal static class CodecExtensions
{
    public static bool IsCustomerCodec(this Codec c)
    {
        return c is >= (Codec) CustomerCodecs.CodecCustomerStart and <= (Codec) CustomerCodecs.CodecCustomerEnd;
    }

    public static Ydb.Topic.Codec ToProto(this Codec c)
    {
        return (Ydb.Topic.Codec) c;
    }
}
