using Ydb.Sdk.GrpcWrappers.Topic.Extensions;
using Ydb.Sdk.Utils;
using PublicCodec = Ydb.Sdk.Services.Topic.Models.Codec;

namespace Ydb.Sdk.GrpcWrappers.Topic.Codecs;

internal class SupportedCodecs: List<Codec>
{
    public SupportedCodecs(IEnumerable<Codec> collection) : base(collection)
    {
    }

    public static SupportedCodecs FromProto(Ydb.Topic.SupportedCodecs source)
    {
        return new SupportedCodecs(source.Codecs.Select(c => (Codec) c));
    }

    public static SupportedCodecs FromPublic(IEnumerable<PublicCodec> codecs)
        => new(codecs.Select(EnumConverter.Convert<PublicCodec, Codec>));

    public IEnumerable<PublicCodec> ToPublic()
        => this.Select(EnumConverter.Convert<Codec, PublicCodec>);
    
    public bool IsAllowedCodec(Codec codec) => !this.Any() || Contains(codec);

    public SupportedCodecs Clone() => new(this);

    public Ydb.Topic.SupportedCodecs ToProto()
    {
        var result = new Ydb.Topic.SupportedCodecs();
        result.Codecs.AddRange(this.Select(c => (int)c.ToProto()));
        return result;
    }
}
