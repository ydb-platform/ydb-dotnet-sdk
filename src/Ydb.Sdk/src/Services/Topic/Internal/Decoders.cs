using System.IO.Compression;
using Ydb.Sdk.Services.Topic.Models;
using Codec = Ydb.Sdk.GrpcWrappers.Topic.Codecs.Codec;

namespace Ydb.Sdk.Services.Topic.Internal;

internal class Decoders
{
    private readonly Dictionary<Codec, Func<byte[], byte[]>> decoders;

    public Decoders()
    {
        decoders = new Dictionary<Codec, Func<byte[], byte[]>>
        {
            {Codec.Raw, data => data},
            {Codec.Gzip, Gzip}
        };

        byte[] Gzip(byte[] data)
        {
            var gzippedDataStream = new MemoryStream();
            using var gzipStream = new GZipStream(gzippedDataStream, CompressionMode.Compress);
            gzipStream.Write(data);

            return gzippedDataStream.ToArray();
        }
    }

    //TODO IDecoder to add like (data) => decoder.Decode(data) ? 
    public void Add(Codec codec, Func<byte[], byte[]> decode) => decoders[codec] = decode;

    public byte[] Decode(Codec codec, byte[] data)
    {
        if (!decoders.TryGetValue(codec, out var decode))
            /*TODO*/ ;

        return decode(data);
    }
}
