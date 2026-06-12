using System.Text.Json;

namespace CoordinationService;

public sealed record CoordinationPayload(long Version, string WriterId, DateTimeOffset UpdatedAt)
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public static byte[] Encode(long version, string writerId, DateTimeOffset updatedAt) =>
        JsonSerializer.SerializeToUtf8Bytes(new CoordinationPayload(version, writerId, updatedAt), JsonOptions);

    public static CoordinationPayload Decode(byte[] data)
    {
        if (data.Length == 0)
        {
            return new CoordinationPayload(0, "", DateTimeOffset.UnixEpoch);
        }

        var payload = JsonSerializer.Deserialize<CoordinationPayload>(data, JsonOptions);
        if (payload == null)
        {
            throw new CoordinationSloInvariantException("Coordination payload is empty or invalid JSON.");
        }

        return payload;
    }
}
