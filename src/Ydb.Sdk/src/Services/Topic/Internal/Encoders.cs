using System.IO.Compression;
using Ydb.Sdk.GrpcWrappers.Topic.Codecs;

namespace Ydb.Sdk.Services.Topic.Internal;

internal class Encoders
{
    private readonly Dictionary<Codec, Func<byte[], byte[]>> encoders;

    public Encoders()
    {
        encoders = new Dictionary<Codec, Func<byte[], byte[]>>
        {
            {Codec.Raw, data => data},
            {Codec.Gzip, Gzip}
        };

        byte[] Gzip(byte[] data)
        {
            var gzippedDataStream = new MemoryStream();
            using var gzipStream = new GZipStream(gzippedDataStream, CompressionMode.Decompress);
            gzipStream.Write(data);

            return gzippedDataStream.ToArray();
        }
    }

    //TODO IDecoder to add like (data) => decoder.Decode(data) ? 
    public void Add(Codec codec, Func<byte[], byte[]> decode) => encoders[codec] = decode;

    public byte[] Encode(Codec codec, byte[] data)
    {
        if (!encoders.TryGetValue(codec, out var encode))
            /*TODO*/ ;

        return encode(data);
    }
}
