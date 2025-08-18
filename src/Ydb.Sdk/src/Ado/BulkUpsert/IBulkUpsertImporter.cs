using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado.BulkUpsert;

public interface IBulkUpsertImporter
{
    /// <summary>Adds one line to the batch.</summary>
    /// <param name="row">The column values are in order <c>columns</c>.</param>
    ValueTask AddRowAsync(params object[] row);
    
    /// <summary>
    /// Adds multiple lines with a single parameter <see cref="YdbList"/>.
    /// </summary>
    /// <remarks>
    /// Expected <c>List&lt;Struct&lt;...&gt;&gt;</c>, where the names and order of the fields are the same as <c>columns</c>.
    /// Form Example: <c>List&lt;Struct&lt;Id:Int64, Name:Utf8&gt;&gt;</c>.
    /// </remarks>
    ValueTask AddListAsync(YdbList list);

    ValueTask FlushAsync();
}
