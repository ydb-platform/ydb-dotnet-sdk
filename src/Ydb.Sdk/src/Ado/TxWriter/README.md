# TxWriter API Usage Example

This example demonstrates how to use the transactional topic writer (TxWriter) API to publish messages to a YDB topic within the same ACID transaction that modifies tables.

## Basic Usage

```csharp
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.TxWriter;

// Create and open connection
using var connection = new YdbConnection(connectionString);
await connection.OpenAsync();

// Begin a transaction
using var transaction = connection.BeginTransaction();

try
{
    // Create a transactional writer for a topic
    var txWriter = connection.CreateTxWriter<string>("my-topic");

    // Perform table operations
    var command = connection.CreateCommand();
    command.Transaction = transaction;
    command.CommandText = @"
        INSERT INTO Users (Id, Name, Email) 
        VALUES (@Id, @Name, @Email)
    ";
    command.Parameters.AddWithValue("$Id", 1);
    command.Parameters.AddWithValue("$Name", "John Doe");
    command.Parameters.AddWithValue("$Email", "john@example.com");
    await command.ExecuteNonQueryAsync();

    // Write messages to the topic (non-blocking)
    txWriter.Write("User created: John Doe");
    txWriter.Write("Notification sent");

    // Commit transaction - automatically flushes all pending messages
    // Messages become visible atomically with table changes
    await transaction.CommitAsync();
}
catch (Exception)
{
    // On error, rollback will clean up pending messages
    await transaction.RollbackAsync();
    throw;
}
```

## Multiple Topics

```csharp
using var connection = new YdbConnection(connectionString);
await connection.OpenAsync();

using var transaction = connection.BeginTransaction();

// Create writers for multiple topics
var notificationWriter = connection.CreateTxWriter<string>("notifications");
var auditWriter = connection.CreateTxWriter<string>("audit-log");

// Perform table operations
// ...

// Write to different topics
notificationWriter.Write("Email notification queued");
auditWriter.Write("Action: User created");
notificationWriter.Write("SMS notification queued");

// All messages are flushed and committed atomically
await transaction.CommitAsync();
```

## Custom Options

```csharp
// Configure writer options
var options = new TxWriterOptions
{
    BufferMaxSize = 10 * 1024 * 1024,  // 10 MB buffer
    ProducerId = "my-app-producer-1",   // For deduplication
    PartitionId = 1                      // Specific partition
};

var txWriter = connection.CreateTxWriter<string>("my-topic", options);
```

## Buffer Overflow Handling

```csharp
var options = new TxWriterOptions 
{ 
    BufferMaxSize = 1024 * 1024  // 1 MB buffer
};

var txWriter = connection.CreateTxWriter<string>("my-topic", options);

try
{
    // Write many large messages
    for (int i = 0; i < 1000; i++)
    {
        txWriter.Write(largeMessage);
    }
}
catch (TxTopicWriterException ex)
{
    // Handle buffer overflow
    Console.WriteLine($"Buffer full: {ex.Message}");
    
    // Options:
    // 1. Commit current transaction to flush buffer
    // 2. Increase BufferMaxSize
    // 3. Write smaller messages
    // 4. Use multiple transactions
}
```

## Error Handling

```csharp
using var connection = new YdbConnection(connectionString);
await connection.OpenAsync();

using var transaction = connection.BeginTransaction();

var txWriter = connection.CreateTxWriter<string>("my-topic");

try
{
    // Table operations
    await command.ExecuteNonQueryAsync();
    
    // Topic writes
    txWriter.Write("Message 1");
    txWriter.Write("Message 2");
    
    // If commit fails, all changes (table + topic) are rolled back
    await transaction.CommitAsync();
}
catch (YdbException ex) when (ex.Code == StatusCode.Aborted)
{
    // Transaction was aborted - retry logic
    Console.WriteLine("Transaction aborted, retrying...");
}
catch (TxTopicWriterException ex)
{
    // Topic writer specific error
    Console.WriteLine($"Topic writer error: {ex.Message}");
}
```

## Key Points

1. **Transaction Required**: You must start a transaction before creating a TxWriter.
2. **Automatic Flush**: Messages are automatically flushed before commit.
3. **Atomic Visibility**: Messages become visible only after successful commit.
4. **Buffer Management**: Monitor buffer size to avoid overflow exceptions.
5. **Error Handling**: Both table and topic operations can fail; handle appropriately.
6. **Cleanup**: Writers are automatically disposed on commit or rollback.

## Common Patterns

### Outbox Pattern Replacement

Before (manual outbox pattern):
```csharp
// Insert into outbox table
INSERT INTO outbox (id, topic, message) VALUES (...)

// Separate background worker reads outbox and publishes
```

After (with TxWriter):
```csharp
// Direct transactional write to topic
var txWriter = connection.CreateTxWriter<string>("my-topic");
txWriter.Write("message");
await transaction.CommitAsync();
```

### Saga Coordination

```csharp
// Coordinate distributed transaction with topic messages
var sagaWriter = connection.CreateTxWriter<SagaEvent>("saga-coordinator");

// Local transaction
await UpdateLocalState();
sagaWriter.Write(new SagaEvent { Type = "OrderCreated", OrderId = orderId });

// Commit coordinates the saga step
await transaction.CommitAsync();
```
