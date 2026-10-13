namespace Ydb.Sdk.Ado.Internal;

internal static class StatsModeExtensions
{
    internal static Query.StatsMode ToProto(this StatsMode mode) => mode switch
    {
        StatsMode.None => Query.StatsMode.None,
        StatsMode.Basic => Query.StatsMode.Basic,
        StatsMode.Full => Query.StatsMode.Full,
        StatsMode.Profile => Query.StatsMode.Profile,
        StatsMode.Unspecified => Query.StatsMode.Unspecified,
        _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null)
    };
}
