using Ydb.Sdk.Client;
using Ydb.Table;
using Ydb.Table.V1;

namespace Ydb.Sdk.Services.Table;

public class RenameTableItem
{
    public string SourcePath { get; }
    public string DestinationPath { get; }
    public bool ReplaceDestination { get; }

    public RenameTableItem(string sourcePath, string destinationPath, bool replaceDestination)
    {
        SourcePath = sourcePath;
        DestinationPath = destinationPath;
        ReplaceDestination = replaceDestination;
    }

    public Ydb.Table.RenameTableItem GetProto(TableClient tableClient)
    {
        return new Ydb.Table.RenameTableItem
        {
            SourcePath = tableClient.MakeTablePath(SourcePath),
            DestinationPath = tableClient.MakeTablePath(DestinationPath),
            ReplaceDestination = ReplaceDestination
        };
    }
}

public class RenameTablesSettings : OperationRequestSettings
{
}

public class RenameTablesResponse : ResponseBase
{
    internal RenameTablesResponse(Status status) : base(status)
    {
    }
}

public partial class TableClient
{
    public async Task<RenameTablesResponse> RenameTables(IEnumerable<RenameTableItem> tableItems,
        RenameTablesSettings? settings = null)
    {
        settings ??= new RenameTablesSettings();
        var request = new RenameTablesRequest
        {
            OperationParams = MakeOperationParams(settings)
        };
        request.Tables.AddRange(tableItems.Select(item => item.GetProto(this)));

        try
        {
            var response = await Driver.UnaryCall(
                method: TableService.RenameTablesMethod,
                request: request,
                settings: settings);

            var status = UnpackOperation(response.Data.Operation);
            return new RenameTablesResponse(status);
        }
        catch (Driver.TransportException e)
        {
            return new RenameTablesResponse(e.Status);
        }
    }
}
