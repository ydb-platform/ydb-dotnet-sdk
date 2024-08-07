using System.Text;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Ydb.Sdk.GrpcWrappers.Topic.Writer.Write;
using Ydb.Topic;

namespace Ydb.Sdk.GrpcWrappers.Topic.Writer;

internal class WriterUtils
{
    private const int DefaultMaxMessageSize = 64 * 1000_000;
    private static readonly int MessageDataOverhead = GetMessageDataOverhead();

    public static List<WriteRequest> MessagesToWriteRequests(List<Message> messages)
    {
        var groups = SplitMessagesForSend(messages);

        return groups.Select(group => new WriteRequest
        {
            Messages = group,
            Codec = group.First().Codec
        }).ToList();
    }

    private static List<List<Message>> SplitMessagesForSend(List<Message> messages)
    {
        var messagesGroupedByCodec = messages
            .GroupBy(m => m.Codec)
            .Select(g => g.ToList())
            .ToList();

        var result = new List<List<Message>>();
        foreach (var codecGroup in messagesGroupedByCodec)
        {
            var messagesGroupedBySize = SplitMessagesBySize(codecGroup, DefaultMaxMessageSize);
            result.AddRange(messagesGroupedBySize);
        }

        return result;
    }

    private static List<List<Message>> SplitMessagesBySize(List<Message> messages, int splitSize)
    {
        var result = new List<List<Message>>();
        var group = new List<Message>();
        var groupSize = 0;

        foreach (var message in messages)
        {
            var messageSize = GetMessageSize(message);

            if (group.Count == 0 || groupSize + messageSize <= splitSize)
            {
                group.Add(message);
                groupSize += messageSize;
            }
            else
            {
                result.Add(group);
                group = new List<Message> {message};
                groupSize = messageSize;
            }
        }

        if (group.Count > 0)
            result.Add(group);

        return result;
    }

    private static int GetMessageSize(Message message) => message.Data.Length + MessageDataOverhead;

    private static int GetMessageDataOverhead()
    {
        return new StreamWriteMessage.Types.FromClient
        {
            WriteRequest = new StreamWriteMessage.Types.WriteRequest
            {
                Messages =
                {
                    new[]
                    {
                        new StreamWriteMessage.Types.WriteRequest.Types.MessageData
                        {
                            SeqNo = int.MaxValue,
                            CreatedAt = DateTime.MaxValue.ToTimestamp(),
                            Data = ByteString.CopyFrom(1),
                            UncompressedSize = int.MaxValue,
                            MessageGroupId = Encoding.UTF8.GetString(new byte[100])
                        }
                    }
                },
                Codec = 20_000
            }
        }.CalculateSize();
    }
}
