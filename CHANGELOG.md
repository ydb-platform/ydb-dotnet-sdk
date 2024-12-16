## v0.9.0
- Writer client for YDB topics
- Fix: delete default timeout grpc.deadline 

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
  * Fixed setting the SeqNo field in the message (in-flight buffer already has a seqNo) and added a check on canceled TCS.
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
