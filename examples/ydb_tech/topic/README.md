# Topic examples for ydb.tech

These examples are the source of the .NET snippets in the topic reference on ydb.tech.
They use the SDK from this checkout and create temporary topics with unique names.

Start a local YDB instance, then run from the repository root:

```sh
dotnet run --project examples/ydb_tech/topic/Topic.csproj
```

Set `YDB_CONNECTION_STRING` to an ADO.NET connection string to select another database.
The default is `Host=localhost;Port=2136;Database=/local`.
Any failed operation or unexpected payload causes a nonzero exit.
