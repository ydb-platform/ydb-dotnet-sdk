using System.Data;
using Xunit;
using Ydb.Sdk.Ado.Tests.Utils;
using Ydb.Sdk.Ado.TxWriter;

namespace Ydb.Sdk.Ado.Tests;

public class TxWriterTests : TestBase
{
    private static readonly TemporaryTables<TxWriterTests> Tables = new();

    [Fact]
    public void CreateTxWriter_WithoutActiveTransaction_ThrowsInvalidOperationException()
    {
        using var connection = CreateOpenConnection();

        var ex = Assert.Throws<InvalidOperationException>(() => 
            connection.CreateTxWriter<string>("test-topic"));
        
        Assert.Contains("transaction must be active", ex.Message);
    }

    [Fact]
    public void CreateTxWriter_WithActiveTransaction_ReturnsWriter()
    {
        using var connection = CreateOpenConnection();
        using var transaction = connection.BeginTransaction();

        var writer = connection.CreateTxWriter<string>("test-topic");

        Assert.NotNull(writer);
        Assert.IsAssignableFrom<ITxTopicWriter<string>>(writer);
    }

    [Fact]
    public void Write_SingleMessage_EnqueuesSuccessfully()
    {
        using var connection = CreateOpenConnection();
        using var transaction = connection.BeginTransaction();

        var writer = connection.CreateTxWriter<string>("test-topic");

        // Should not throw
        writer.Write("test message");
    }

    [Fact]
    public void Write_MultipleMessages_EnqueuesSuccessfully()
    {
        using var connection = CreateOpenConnection();
        using var transaction = connection.BeginTransaction();

        var writer = connection.CreateTxWriter<string>("test-topic");

        for (int i = 0; i < 10; i++)
        {
            writer.Write($"test message {i}");
        }
    }

    [Fact]
    public void Write_AfterTransactionCompleted_ThrowsInvalidOperationException()
    {
        using var connection = CreateOpenConnection();
        var transaction = connection.BeginTransaction();
        var writer = connection.CreateTxWriter<string>("test-topic");
        
        transaction.Commit();

        var ex = Assert.Throws<InvalidOperationException>(() => 
            writer.Write("test message"));
        
        Assert.Contains("transaction is already completed", ex.Message);
    }

    [Fact]
    public void Write_ExceedsBufferSize_ThrowsTxTopicWriterException()
    {
        using var connection = CreateOpenConnection();
        using var transaction = connection.BeginTransaction();

        var options = new TxWriterOptions { BufferMaxSize = 100 }; // Small buffer
        var writer = connection.CreateTxWriter<string>("test-topic", options);

        // First message should succeed
        writer.Write("small");

        // Second message should exceed buffer and throw
        var largeMessage = new string('x', 200);
        var ex = Assert.Throws<TxTopicWriterException>(() => 
            writer.Write(largeMessage));
        
        Assert.Contains("Buffer overflow", ex.Message);
    }

    [Fact]
    public async Task CommitAsync_WithPendingMessages_FlushesBeforeCommit()
    {
        using var connection = CreateOpenConnection();
        var transaction = connection.BeginTransaction();

        var writer = connection.CreateTxWriter<string>("test-topic");
        writer.Write("message 1");
        writer.Write("message 2");
        writer.Write("message 3");

        // Should flush messages before committing
        await transaction.CommitAsync();

        // Transaction should be completed
        Assert.True(transaction.Completed);
    }

    [Fact]
    public async Task RollbackAsync_WithPendingMessages_DisposesWriters()
    {
        using var connection = CreateOpenConnection();
        var transaction = connection.BeginTransaction();

        var writer = connection.CreateTxWriter<string>("test-topic");
        writer.Write("message 1");
        writer.Write("message 2");

        await transaction.RollbackAsync();

        // Transaction should be completed
        Assert.True(transaction.Completed);
        
        // Writers should be disposed
        Assert.Empty(connection.TxWriters);
    }

    [Fact]
    public async Task CommitAsync_WithMultipleWriters_FlushesAllWriters()
    {
        using var connection = CreateOpenConnection();
        var transaction = connection.BeginTransaction();

        var writer1 = connection.CreateTxWriter<string>("test-topic-1");
        var writer2 = connection.CreateTxWriter<string>("test-topic-2");

        writer1.Write("message 1 to topic 1");
        writer2.Write("message 1 to topic 2");
        writer1.Write("message 2 to topic 1");
        writer2.Write("message 2 to topic 2");

        // Should flush all writers before committing
        await transaction.CommitAsync();

        // Transaction should be completed
        Assert.True(transaction.Completed);
        
        // Writers should be disposed
        Assert.Empty(connection.TxWriters);
    }

    [Fact]
    public async Task CommitAsync_WithTableUpdatesAndTopicWrites_CompletesAtomically()
    {
        using var connection = CreateOpenConnection();
        var transaction = connection.BeginTransaction();

        // Do a table update
        var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $@"
            UPSERT INTO {Tables.Seasons} (series_id, season_id, first_aired) 
            VALUES (1, 99, Date(""2024-01-01""));
        ";
        command.ExecuteNonQuery();

        // Write to topic
        var writer = connection.CreateTxWriter<string>("test-topic");
        writer.Write("synchronized message");

        // Commit should succeed
        await transaction.CommitAsync();

        // Verify table update was applied
        var verifyCommand = connection.CreateCommand();
        verifyCommand.CommandText = $@"
            SELECT first_aired FROM {Tables.Seasons} WHERE series_id = 1 AND season_id = 99
        ";
        var result = verifyCommand.ExecuteScalar();
        Assert.Equal(new DateTime(2024, 1, 1), result);
    }

    [Fact]
    public void CreateTxWriter_WithCustomOptions_UsesOptions()
    {
        using var connection = CreateOpenConnection();
        using var transaction = connection.BeginTransaction();

        var options = new TxWriterOptions
        {
            BufferMaxSize = 1024 * 1024, // 1 MB
            ProducerId = "test-producer",
            PartitionId = 1
        };

        var writer = connection.CreateTxWriter<string>("test-topic", options);

        Assert.NotNull(writer);
    }

    [Fact]
    public void Write_AllowsSingleLargeMessageEvenIfExceedsBuffer()
    {
        using var connection = CreateOpenConnection();
        using var transaction = connection.BeginTransaction();

        var options = new TxWriterOptions { BufferMaxSize = 100 }; // Small buffer
        var writer = connection.CreateTxWriter<string>("test-topic", options);

        // A single large message should be allowed (first message exception)
        var largeMessage = new string('x', 200);
        writer.Write(largeMessage);

        // But the next message should fail
        var ex = Assert.Throws<TxTopicWriterException>(() => 
            writer.Write("next message"));
        
        Assert.Contains("Buffer overflow", ex.Message);
    }
}
