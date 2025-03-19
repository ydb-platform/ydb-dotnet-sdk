using Ydb.Sdk.Services.Table;

namespace Ydb.Sdk.Examples;

public class TableExampleBase
{
    protected TableClient Client { get; }
    protected string BasePath { get; }

    protected TableExampleBase(TableClient client, string database, string path)
    {
        Client = client;
        BasePath = string.Join('/', database, path);
    }

    protected string FullTablePath(string table) => string.Join('/', BasePath, table);
}