- **Breaking Change**: Removed unused methods `GetJson`, `GetJsonDocument`, and `GetYson` from YdbDataReader.
- Feat ADO.NET: Add support for reading `Yson` from `YdbDataReader.GetBytes`.
- Feat ADO.NET: Add support for reading `Json` and `JsonDocument` from `YdbDataReader.GetString`.
- Feat ADO.NET: Added type checking in the parameter list in sql statement `IN (@id1, @id2)`.
- **Breaking Change**: `Ydb.Sdk.Services.Topic` moved to `Ydb.Sdk.Topic`.

## v0.24.0

- **Breaking Change**: Renamed properties in `YdbConnectionStringBuilder`:
  - `MaxSessionPool` -> `MaxPoolSize`.
  - `MinSessionPool` -> `MinPoolSize`.
- Added XML documentation for all public APIs in `Ydb.Sdk`.
- Feat ADO.NET: Added dispose timeout (10 seconds) to `PoolingSessionSource`.
- Feat ADO.NET: Added `EnableImplicitSession` to support implicit sessions.

## v0.23.1

- Fixed bug Topic Reader: NullReferenceException when handling StopPartitionSessionRequest ([#528](https://github.com/ydb-platform/ydb-dotnet-sdk/issues/528)).
- Feat ADO.NET: Added YSON type support (YdbDbType.Yson) with byte[] values.

## v0.23.0

- Feat ADO.NET: `YdbDataSource.OpenRetryableConnectionAsync` opens a retryable connection with automatic retries for transient failures.
- Fixed bug ADO.NET/PoolManager: `SemaphoreSlim.WaitAsync` over-release on cancellation.
- Feat ADO.NET: Mark `YdbConnection.State` as `Broken` when the underlying session is broken, including background deactivation.
- Feat ADO.NET: Added  YdbDataSource `ExecuteAsync` and `ExecuteInTransaction` convenience methods.
- **Breaking Change**: `Ydb.Sdk.Services.Query.TxMode` moved to `Ydb.Sdk.Ado.TransactionMode`.
- Feat ADO.NET: Cache gRPC transport by `gRPCConnectionString` to reuse channels.
- Fixed bug wrap-around ADO.NET: Big parameterized Decimal — `((ulong)bits[1] << 32)` -> `((ulong)(uint)bits[1] << 32)`.
- Feat ADO.NET: Parameterized Decimal overflow check: `Precision` and `Scale`.
- Feat ADO.NET: Deleted support for `DateTimeOffset` was a mistake.
- Feat ADO.NET: Added support for `Date32`, `Datetime64`, `Timestamp64` and `Interval64` types in YDB.
- Feat: Implement `YdbRetryPolicy` with AWS-inspired Exponential Backoff and Jitter.
- Dev: LogLevel `Warning` -> `Debug` on DeleteSession has been `RpcException`.

## v0.22.0

- Added `YdbDbType` property to `YdbParameter`, allowing to explicitly specify YDB-specific data types for parameter mapping.
- ADO.NET: Now `YdbConnection.OpenAsync` and `YdbCommand.Execute*` throw `OperationCanceledException`,
  if the CancellationToken has already been cancelled before the method is called.
- Feat ADO.NET: decimal type with arbitrary precision/scale ([#498](https://github.com/ydb-platform/ydb-dotnet-sdk/issues/498)).
- Fixed bug: interval value parsing in microseconds and double instead of ticks ([#497](https://github.com/ydb-platform/ydb-dotnet-sdk/issues/497)).
- ADO.NET: Changed `IBulkUpsertImporter.AddRowAsync` signature: `object?[] row` → `params object[]`.

## v0.21.0

- ADO.NET: Added `MinPoolSize` setting to keep a minimum number of sessions ready in the PoolingSessionSource.
- ADO.NET: Added `SessionIdleTimeout` to remove idle sessions from the PoolingSessionSource automatically.
- ADO.NET: Made `PoolingSessionSource` faster and more reliable by using a lock-free LIFO stack.
- ADO.NET: Added `BeginBulkUpsertImport` for batch upsert operations with transaction checks.
- Optimization: On BadSession, do not invoke the `DeleteSession()` method.
- Canceling AttachStream after calling the `DeleteSession` method.
- Fixed bug: fixed issue where session was not deleted (`ClientTransportTimeout`).
- Fixed bug: Grpc.Core.StatusCode.Cancelled was mapped to server's Canceled status.
- Feat ADO.NET: PoolingSessionSource 2.0 based on lock-free FIFO pooling algorithm.
- Added new ADO.NET options:
  - `MinPoolSize`: The minimum session pool size.
  - `SessionIdleTimeout`: The time (in seconds) to wait before closing idle session in the pool if the count of all sessions exceeds `MinPoolSize`.
- Fixed bug `Reader`: unhandled exception in `TryReadRequestBytes(long bytes)`.
- Handle `YdbException` on `DeleteSession`.
- Do not invoke `DeleteSession` if the session is not active.
- `YdbException`: Added cancellation token propagation support in `CommitAsync` and `RollbackAsync`.
- Deleted legacy exceptions: Driver.TransportException, StatusUnsuccessfulException and InitializationFailureException.
- Fixed bug: Unhandled exception System.Net.Http.HttpIOException has now been converted to YdbException ([grpc-dotnet issue](https://github.com/grpc/grpc-dotnet/issues/2638)).
- Added 'x-ydb-client-pid' header to any RPC calls.
- Added DisableServerBalancer option to ADO.NET session creation; default false.

## v0.20.1

- Fixed bug ADO.NET: `YdbSchema.SchemaObjects` and `Ydb.DescribeTable`methods are public for `EntityFrameworkCore.Ydb`.

## v0.20.0

- Fixed bug: SQL parser skips token after param. 
- ADO.NET: Added support for conversion from IN (?, ?, ?) to IN $list ([#447](https://github.com/ydb-platform/ydb-dotnet-sdk/issues/447)).

## v0.19.0

- ADO.NET: session is now deactivated when cancelled.
- Fixed bug ADO.NET: throws an `InvalidOperationException` if the connection is broken during the next invocation.
- Fixed bug `YdbCommand`: `Execute*` methods now propagate the cancellation token only for initializing YdbDataReader; the token is not passed to the server stream.
- `YdbCommand`: Improved cancellation token propagation in `Execute*` methods.
- `YdbConnection`: Added cancellation token propagation support in `OpenAsync`.
- `YdbDataReader`: Added cancellation token propagation support in `ReadAsync` and `NextResultAsync`.
- Added `CreateSessionTimeout` option to ADO.NET session creation; default is 5 seconds.

## v0.18.3

- Added `ConnectTimeout`: time to wait (in seconds) while trying to establish a connection.

## v0.18.2

- Fixed YdbException: propagate inner exception.

## v0.18.1

- Fixed bug: 'System.DateOnly' is not supported by YdbParameter ([#449](https://github.com/ydb-platform/ydb-dotnet-sdk/issues/449)).
- Fixed bug: Unhandled exception.
  System.Net.Http.HttpIOException ([#452](https://github.com/ydb-platform/ydb-dotnet-sdk/issues/451)).
- dev: LogLevel `Warning` -> `Debug` on AttachStream has been cancelled.

## v0.18.0

- Disable Discovery mode: skip discovery step and client balancing and use connection to start
  endpoint ([#420](https://github.com/ydb-platform/ydb-dotnet-sdk/issues/420)).

## v0.17.0

- Shutdown channels which are removed from the EndpointPool after discovery calls.
- Fixed bug: Received message exceeds the maximum configured message
  size ([#421](https://github.com/ydb-platform/ydb-dotnet-sdk/issues/421)).
- Added `MaxSendMessageSize` \ `MaxReceiveMessageSize` grpc message size settings.
- Added `EnableMultipleHttp2Connections` setting to grpc channel.
- `Connection.State` is set to `Broken` when the session is deactivated.

## v0.16.2

- Fixed bug in method GetSchema(): collection columns return Unspecified when decimal type about column.

## v0.16.1

- Added `x-ydb-sdk-build-info` header to any RPC call.

## v0.16.0

- **Breaking Change**: `Ydb.Sdk.Yc.Auth` version <= 0.1.0 is not compatible with newer versions.
- Added `IAuthClient` to fetch auth token.
- Added the `CachedCredentialsProvider` class, which streamlines token lifecycle management.
- **Breaking Change**: Deleted `AnonymousProvider`. Now users don't need to do anything for anonymous authentication.
  Migration guide:
  ```c#
  var config = new DriverConfig(...); // Using AnonymousProvider if Credentials property is null
  ```
- **Breaking Change**: Deleted `StaticCredentialsProvider`. Users are now recommended to use the `User` and `Password`
  properties in `DriverConfig` for configuring authentication. Migration guide:
  ```c#
  var config = new DriverConfig(...)
  {
      User = "your_username",
      Password = "your_password"
  };
  ```

## v0.15.4

- Added `KeepAlivePingTimeout`, with a default value of 10 seconds.
- Added `KeepAlivePingDelay`, with a default value of 10 seconds.

## v0.15.3

- Added SeqNo to `Ydb.Sdk.Services.Topic.Reader.Message`.

## v0.15.2

- Added SeqNo to `WriteResult`.
- Changed signature of the `TopicClient.DropTopic` method.

## v0.15.1

- Fixed Writer: possible creation of a session after `DisposeAsync()`, which this could happen when there are canceled
  tasks in `InFlightMessages`.
- Dev: `Writer.MoveNext()` changed exception on cancelToken from `WriterException` to `TaskCanceledException`.
- Dev: changed log level from `Warning` to `Information` in `(Reader / Writer).Initialize()` when it is disposed.

## v0.15.0

- Dev: added `ValueTask<string?> GetAuthInfoAsync()` in ICredentialProvider.
- Feat: `Writer.DisposeAsync()` waits for all in-flight messages to complete.
- Feat: `Reader.DisposeAsync()` waits for all pending commits to be completed.
- **Breaking Change**: `IReader` now implements `IAsyncDisposable` instead of `IDisposable`.  
  This change requires updates to code that disposes `IReader` instances. Use `await using` instead of `using`.
- **Breaking Change**: `IWriter` now implements `IAsyncDisposable` instead of `IDisposable`.  
  This change requires updates to code that disposes `IWriter` instances. Use `await using` instead of `using`.
- Topic `Reader` & `Writer`: update auth token in bidirectional stream.

## v0.14.1

- Fixed bug: public key presented not for certificate signature.
- Fixed: YdbDataReader does not throw YdbException when CloseAsync is called for UPDATE/INSERT statements with no
  result.

## v0.14.0

- Reader client for YDB topics
- Fixed: send PartitionIds in InitRequest.
- Do a committed offset on StopPartitionSessionRequest event anyway.
- Added log info on StopPartitionSessionRequest event.
- PartitioningSettings were changed to change the PartitionCountLimit to MaxActivePartitions.
- Dev: updated System.IdentityModel.Tokens.Jwt from version 0.7.0 to version 8.5.0.
- PartitionSession.Stop uses committedOffset to complete commit tasks.
- Changed batch type: IReadOnlyCollection<Message<TValue>> -> IReadOnlyList<Message<TValue>>.
- Invoking TryReadRequestBytes before deserializing message.
- Updated Ydb.Protos 1.0.6 -> 1.1.1: Updated version of the Grpc.Net.Client library to 2.67.0 and proto messages.
- Fixed: YdbDataReader.GetDataTypeName for optional values.
- Added support for "Columns" collectionName in YdbConnection.GetSchema(Async).

## v0.12.0

- GetUint64(int ordinal) returns a ulong for Uint8, Uint16, Uint32, Uint64 YDB types.
- GetInt64(int ordinal) returns a int for Int8, Int16, Int32, Int64, Uint8, Uint16, Uint32 YDB types.
- GetUint32(int ordinal) returns a uint for Uint8, Uint16, Uint32 YDB types.
- GetInt32(int ordinal) returns a int for Int8, Int16, Int32, Uint8, Uint16 YDB types.
- GetUint16(int ordinal) returns a ushort for Uint8, Uint16 YDB types.
- GetInt16(int ordinal) returns a short for Int8, Int16, Uint8 YDB types.
- GetDouble(int ordinal) returns a double for Float and Double YDB types.
- Throw InvalidCastException on string.Empty in `GetChar(int ordinal)`.
- Changed Ydb.Sdk.Value.InvalidTypeException to InvalidCastException in YdbValueParser.
- Changed InvalidCastException to InvalidOperationException in YdbParameter.
- Added specification tests: YdbCommandTests and YdbParameterTests.
- YdbConnection.Database returns string.Empty if ConnectionStringBuilder is null.
- Propagated cancellationToken in Execute[.*]Async methods.
- When YdbCommand has an open data reader, it throws InvalidOperationException on the setters: CommandText,
  DbConnection.
- Added checkers to YdbCommand.Prepare().
- CommandText getter doesn't throw an exception if the CommandText property has not been initialized.

## v0.11.0

- Fix bug: GetValue(int ordinal) return DBNull.Value if fetched NULL value.
- Fix: NextResult() moves to the next result and skip the first ResultSet.
- Added specification DbDataReaderTests.
- If dataOffset is larger than the length of data, GetChars and GetBytes methods will return 0.
- If YdbDataReader is closed: `throw new InvalidOperationException("The reader is closed")`.
- InvalidOperationException on ConnectionString property has not been initialized.
- One YdbTransaction per YdbConnection. Otherwise, throw an exception: InvalidOperationException("A transaction is
  already in progress; nested/concurrent transactions aren't supported.").
- ConnectionString returns an empty.String when it is not set.
- When a YdbDataReader is closed, if stream is not empty, a YdbTransaction fails if it is not null. A session also fails
  due to a possible error SessionBusy race condition with the server.
- Fixed bug: Fetch txId from the last result set.
- YdbTransaction CheckDisposed() (invoke rollback if transaction hasn't been committed).
- Dev: Added specification tests for YdbTransaction.

## v0.10.0

- Fixed bug in Topic Writer: race condition when session fails, then write operation starts on previous session and new
  session is created. Messages may be lost.
- Supported in ADO.NET GetSchema(Async). CollectionNames:
    * Tables
    * TablesWithCollections
    * DataSourceInformation
    * MetaDataCollections
    * Restrictions
- Rename field _onStatus -> _onNotSuccessStatus in YdbDataReader
- If session is not active, do not invoke DeleteNotActiveSession(session)
- AttachStream: connect stream using NodeId
- PoolManager: change pool properties on field
- Delete *Settings.DefaultInstance because it's a singleton object that's changed by tasks when NodeId is set
- DbConnection.Session.OnStatus(status) in YdbTransaction

## v0.9.4

- Do not pessimize the node on Grpc.Core.StatusCode.Cancelled and Grpc.Core.StatusCode.DeadlineExceeded.
- Dispose of WriterSession using dispose CancellationToken.
- BidirectionalStream is internal class.
- Move Metadata class to Ydb.Sdk.Services.Topic.
- Fixed memory leak CancellationTokenRegistration.
- Cancel writing tasks after disposing of Writer.

## v0.9.3

- Fixed bug in Topic Writer: worker is stopped by disposeCts
- Fixed bug in sql parser ADO.NET: deduplication declare param in YQL query
- Deleted property BufferOverflowRetryTimeoutMs

## v0.9.2

- Fixed bug: delete deadline grpc timeout on AttachStream

## v0.9.1

- Update log level on AttachStream

## v0.9.0

- Writer client for YDB topics
- Fixed bug: delete default timeout grpc.deadline

## v0.9.0-rc1

- Topic Writer updated release candidate:
    * Do not send messages that have a timeout by cancelToken.
    * If your value serializer throws an exception, this will be wrapped in a WriterException with unspecified status.
    * Added BufferOverflowRetryTimeoutMs to the next try write.
    * Rename _disposeTokenSource -> _disposeCts.
    * Optimize write worker: if (_toSendBuffer.IsEmpty) continue.
    * On RPC errors create DummyWriterSession.
    * Message has been skipped because its sequence number is less than or equal to the last processed server's SeqNo.
    * Calculate the next sequence number from the calculated previous messages.

## v0.9.0-rc0

- Topic Writer release candidate:
    * Updated CAS semantics for enqueuing in the buffer.
    * Processed buffer overflow on WriteAsync.
    * Setting NotStartedWriterSession with a fail reason on RPC and more errors.
    * New initialization strategy for WriterSession (background task).
    * Supported cancellation token for sending tasks.
    * Fixed setting the SeqNo field in the message (in-flight buffer already has a seqNo) and added a check on canceled
      TCS.
    * Using BitConverter for Serializer / Deserializer.
- Fixed: grpc requests go via proxy on Grpc.NET.Client >= 2.44

## v0.8.0

- Fixed bug on commit with fail, no set failed flag for rollback invocation
- Supported UUID (Guid)

## v0.7.3

- Fixed YdbDataReader: extract Json / Yson types

## v0.7.2

- Fixed YdbDataReader: `GetValue()` returns `DbNull.Value` if field is null
- YdbOperationInProgressException extends YdbException

## v0.7.1

- If an error happened in the transaction, allow one empty rollback

## v0.7.0

- Parsed @param then prepared for use with $ prefix (@p -> $p)
- Fully integrated with Dapper

## v0.6.3

- Fixed bug: parameter type mismatch, when setting optional with null

## v0.6.2

- Fixed bug: adding correct placeholders to all logging calls with parameters

## v0.6.1

- Check status of the transport or server for an invalidated session
- Fixed NPE in DescribeTable

## v0.6.0

- ADO.NET over query-service
- Add EndpointPool
- Add SessionPool 2.0

## v0.4.0

- Fix bug: rounding down when inserting a Timestamp YDB type
- ChannelCache has been implemented using ChannelPool and EndpointPool

## v0.3.2

- Make KeepAlive method public for TableClient

## v0.3.1

- Fix error: Access denied without user token

## v0.3.0

- Add rollback transaction API

## v0.2.2

- Passed logger to TxControl.ToProto

## v0.2.1

- Retry discovery on driver initialize

## v0.2.0

- Added MakeTablePath, CopyTable, CopyTables, DescribeTable methods for TableClient
- Add logging for transactions

## v0.1.5

- Fix timeout error on create session
- Fix transport error on delete session

## v0.1.4

- Add exception throwing when results truncated
- lint: add line feed at file end

## v0.1.3

- Add static auth

## v0.1.1

- Add static code analysis
- Add CodeQL analysis
- Add linter
- Apply uniform code style and fix all warnings

## v0.1.0

- Add support of decimal type

## v0.0.9

- Remove support for .NET Core 3.1
- Add support for .NET 7.0
-

## v0.0.8

- Fixed version number

## v0.0.6

- Add methods for castion to c# nullable to YQL Optional
- Add explicit cast operator for some types
- Tests refactoring
- Add Bool type support
