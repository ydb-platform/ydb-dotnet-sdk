using Xunit;

namespace Ydb.Sdk.Ado.Tests;

public class YdbVirtualTimestampTests
{
    [Fact]
    public void CompareTo_UsesUnsignedLexicographicOrder()
    {
        var first = new YdbVirtualTimestamp(new VirtualTimestamp { PlanStep = 1, TxId = ulong.MaxValue });
        var second = new YdbVirtualTimestamp(new VirtualTimestamp { PlanStep = 2, TxId = 0 });
        var third = new YdbVirtualTimestamp(new VirtualTimestamp { PlanStep = 2, TxId = ulong.MaxValue });
        var fourth = new YdbVirtualTimestamp(new VirtualTimestamp { PlanStep = ulong.MaxValue, TxId = 0 });
        IComparable<YdbVirtualTimestamp> comparable = first;

        Assert.True(comparable.CompareTo(second) < 0);
        Assert.True(second.CompareTo(third) < 0);
        Assert.True(third.CompareTo(second) > 0);
        Assert.True(third.CompareTo(fourth) < 0);
        Assert.Equal(0, third.CompareTo(new YdbVirtualTimestamp(
            new VirtualTimestamp { PlanStep = 2, TxId = ulong.MaxValue })));
    }

    [Fact]
    public void CompareTo_NullIsLessThanTimestamp()
    {
        var timestamp = new YdbVirtualTimestamp(new VirtualTimestamp());

        Assert.True(timestamp.CompareTo(null) > 0);
    }

    [Fact]
    public void TimestampsAreImmutableComparableValues()
    {
        var commit = new VirtualTimestamp { PlanStep = 7, TxId = 9 };
        var snapshot = new VirtualTimestamp { PlanStep = 7, TxId = 8 };
        var commitTimestamp = new YdbVirtualTimestamp(commit);
        var snapshotTimestamp = new YdbVirtualTimestamp(snapshot);

        commit.PlanStep = 100;
        snapshot.TxId = 100;

        Assert.Equal((ulong)7, commitTimestamp.PlanStep);
        Assert.Equal((ulong)9, commitTimestamp.TxId);
        Assert.Equal((ulong)8, snapshotTimestamp.TxId);
        Assert.True(snapshotTimestamp.CompareTo(commitTimestamp) < 0);
    }
}
