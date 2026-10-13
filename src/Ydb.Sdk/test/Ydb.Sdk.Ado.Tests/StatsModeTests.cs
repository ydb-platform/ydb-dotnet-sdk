using Xunit;
using Ydb.Sdk.Ado.Internal;

namespace Ydb.Sdk.Ado.Tests;

public class StatsModeTests
{
    [Theory]
    [InlineData(StatsMode.None, Query.StatsMode.None)]
    [InlineData(StatsMode.Basic, Query.StatsMode.Basic)]
    [InlineData(StatsMode.Full, Query.StatsMode.Full)]
    [InlineData(StatsMode.Profile, Query.StatsMode.Profile)]
    [InlineData(StatsMode.Unspecified, Query.StatsMode.Unspecified)]
    public void ToProto_MapsEachMode(StatsMode mode, Query.StatsMode expected) =>
        Assert.Equal(expected, mode.ToProto());

    [Fact]
    public void Command_DefaultStatsModeIsNone()
    {
        using var command = new YdbCommand();

        Assert.Equal(StatsMode.None, command.StatsMode);
    }

    [Fact]
    public void ToProto_RejectsUnknownMode() =>
        Assert.Throws<ArgumentOutOfRangeException>(() => ((StatsMode)(-1)).ToProto());
}
