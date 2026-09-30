using Xunit;
using Ydb.Sdk.Ado.Transaction;

namespace Ydb.Sdk.Ado.Tests;

public class YdbCommitTimestampTests : TestBase
{
    [Fact]
    public void StrictSerializableRW_UsesQueryProtocolMode()
    {
        Assert.Equal(1, (int)TransactionMode.SnapshotRw);
        Assert.Equal(5, (int)TransactionMode.OnlineInconsistentRo);
        Assert.Equal(6, (int)TransactionMode.StrictSerializableRW);

        var settings = TransactionMode.StrictSerializableRW.TransactionSettings();

        Assert.Equal(Ydb.Query.TransactionSettings.TxModeOneofCase.StrictSerializableReadWrite, settings.TxModeCase);
        Assert.NotNull(settings.StrictSerializableReadWrite);
        Assert.Equal(System.Data.IsolationLevel.Serializable,
            new YdbTransaction(new YdbConnection(), TransactionMode.StrictSerializableRW).IsolationLevel);
    }

    [Fact]
    public void CompareTo_UsesUnsignedLexicographicOrder()
    {
        var scope = new object();
        var first = new YdbCommitTimestamp(new VirtualTimestamp { PlanStep = 1, TxId = ulong.MaxValue }, scope);
        var second = new YdbCommitTimestamp(new VirtualTimestamp { PlanStep = 2, TxId = 0 }, scope);
        var third = new YdbCommitTimestamp(new VirtualTimestamp { PlanStep = 2, TxId = ulong.MaxValue }, scope);

        Assert.True(first.CompareTo(second) < 0);
        Assert.True(second.CompareTo(third) < 0);
        Assert.True(third.CompareTo(second) > 0);
        Assert.Equal(0, third.CompareTo(new YdbCommitTimestamp(third.Value, scope)));
        Assert.Equal(ulong.MaxValue, third.Value.TxId);
    }

    [Fact]
    public void CompareTo_RejectsDifferentConnectionOpenings()
    {
        var first = new YdbCommitTimestamp(new VirtualTimestamp { PlanStep = 1 }, new object());
        var second = new YdbCommitTimestamp(new VirtualTimestamp { PlanStep = 2 }, new object());

        Assert.Throws<InvalidOperationException>(() => first.CompareTo(second));
    }

    [Fact]
    public void CompareTo_NullIsLessThanTimestamp()
    {
        var timestamp = new YdbCommitTimestamp(new VirtualTimestamp(), new object());

        Assert.True(timestamp.CompareTo(null) > 0);
    }

    [Fact]
    public async Task CompareTo_RejectsValuesFromReopenedConnection()
    {
        await using var connection = await CreateOpenConnectionAsync();
        var first = new YdbCommitTimestamp(new VirtualTimestamp { PlanStep = 1 }, connection.TimestampScope);

        await connection.CloseAsync();
        await connection.OpenAsync();

        var second = new YdbCommitTimestamp(new VirtualTimestamp { PlanStep = 2 }, connection.TimestampScope);
        Assert.Throws<InvalidOperationException>(() => first.CompareTo(second));
    }

    [Fact]
    public void Value_ReturnsCopy()
    {
        var timestamp = new YdbCommitTimestamp(new VirtualTimestamp { PlanStep = 7, TxId = 9 }, new object());

        timestamp.Value.PlanStep = 100;

        Assert.Equal((ulong)7, timestamp.PlanStep);
        Assert.Equal((ulong)7, timestamp.Value.PlanStep);
    }

    [Fact]
    public void Transaction_ExposesOnlyStrictModeServerTimestamp()
    {
        var connection = new YdbConnection();
        var strictTransaction = new YdbTransaction(connection, TransactionMode.StrictSerializableRW);
        var regularTransaction = new YdbTransaction(connection, TransactionMode.SerializableRw);

        strictTransaction.SetCommitTimestamp(null);
        Assert.Null(strictTransaction.CommitTimestamp);

        var value = new VirtualTimestamp { PlanStep = 8, TxId = 13 };
        strictTransaction.SetCommitTimestamp(value);
        regularTransaction.SetCommitTimestamp(value);

        Assert.Equal((ulong)8, strictTransaction.CommitTimestamp!.Value.PlanStep);
        Assert.Equal((ulong)13, strictTransaction.CommitTimestamp.Value.TxId);
        Assert.Null(regularTransaction.CommitTimestamp);
    }
}
