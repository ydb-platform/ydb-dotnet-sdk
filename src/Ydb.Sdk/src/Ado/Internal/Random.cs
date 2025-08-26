namespace Ydb.Sdk.Ado.Internal;

public interface IRandom
{
    public int Next(int maxValue);
}

internal class ThreadLocalRandom : IRandom
{
    internal static readonly ThreadLocalRandom Instance = new();

    public int Next(int maxValue) => Random.Shared.Next(maxValue);
}
