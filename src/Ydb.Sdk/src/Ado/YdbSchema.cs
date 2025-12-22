using System.Data;
using System.Data.Common;
using System.Globalization;
using Ydb.Scheme;
using Ydb.Scheme.V1;
using Ydb.Sdk.Ado.Internal;
using Ydb.Sdk.Ado.Schema;
using Ydb.Table;
using Ydb.Table.V1;

namespace Ydb.Sdk.Ado;

internal static class YdbSchema
{
    internal static Task<DataTable> GetSchemaAsync(
        YdbConnection ydbConnection,
        string? collectionName,
        string?[] restrictions,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(collectionName);
        if (collectionName.Length == 0)
            throw new ArgumentException("Collection name cannot be empty.", nameof(collectionName));

        return collectionName.ToUpperInvariant() switch
        {
            // Common Schema Collections
            "METADATACOLLECTIONS" => Task.FromResult(GetMetaDataCollections()),
            "DATASOURCEINFORMATION" => GetDataSourceInformation(ydbConnection),
            "RESTRICTIONS" => Task.FromResult(GetRestrictions()),

            // Ydb specific Schema Collections
            "TABLES" => GetTables(ydbConnection, restrictions, cancellationToken),
            "COLUMNS" => GetColumns(ydbConnection, restrictions, cancellationToken),
            "TABLESWITHSTATS" => GetTablesWithStats(ydbConnection, restrictions, cancellationToken),

            _ => throw new ArgumentOutOfRangeException(nameof(collectionName), collectionName,
                "Invalid collection name.")
        };
    }

    internal static Task<IReadOnlyCollection<YdbObject>> SchemaObjects(
        YdbConnection ydbConnection,
        CancellationToken cancellationToken = default
    )
    {
        var database = ydbConnection.Database;

        return SchemaObjects(ydbConnection, WithSuffix(database), database, cancellationToken);
    }

    internal static async Task<YdbTableDescription> DescribeTable(
        IDriver driver,
        string tableName,
        DescribeTableSettings settings,
        CancellationToken cancellationToken
    )
    {
        var describeResponse = await driver.UnaryCall(
            TableService.DescribeTableMethod,
            new DescribeTableRequest
            {
                Path = tableName.FullPath(driver.Database),
                IncludeTableStats = settings.IncludeTableStats
            },
            new GrpcRequestSettings { CancellationToken = cancellationToken }
        );

        if (describeResponse.Operation.Status.IsNotSuccess())
            throw YdbException.FromServer(describeResponse.Operation.Status, describeResponse.Operation.Issues);

        var describeResult = describeResponse.Operation.Result.Unpack<DescribeTableResult>();

        return new YdbTableDescription(tableName, describeResult);
    }

    private static async Task<DataTable> GetTables(
        YdbConnection ydbConnection,
        string?[] restrictions,
        CancellationToken cancellationToken)
    {
        var table = new DataTable("Tables")
        {
            Locale = CultureInfo.InvariantCulture,
            Columns =
            {
                new DataColumn("table_name"),
                new DataColumn("table_type")
            }
        };

        var tableName = restrictions[0];
        var tableType = restrictions[1];

        if (tableName == null) // tableName isn't set
        {
            foreach (var tupleTable in await ListTables(ydbConnection, tableType, cancellationToken))
            {
                table.Rows.Add(tupleTable.TableName, tupleTable.TableType);
            }
        }
        else
        {
            await AppendDescribeTable(
                ydbConnection: ydbConnection,
                tableName: tableName,
                tableType: tableType,
                (_, type) => { table.Rows.Add(tableName, type); },
                cancellationToken: cancellationToken
            );
        }

        return table;
    }

    private static async Task<DataTable> GetTablesWithStats(
        YdbConnection ydbConnection,
        string?[] restrictions,
        CancellationToken cancellationToken)
    {
        var table = new DataTable("TablesWithStats")
        {
            Locale = CultureInfo.InvariantCulture,
            Columns =
            {
                new DataColumn("table_name"),
                new DataColumn("table_type"),
                new DataColumn("rows_estimate", typeof(ulong)),
                new DataColumn("creation_time", typeof(DateTime)),
                new DataColumn("modification_time", typeof(DateTime))
            }
        };

        var tableName = restrictions[0];
        var tableType = restrictions[1];

        if (tableName == null) // tableName isn't set
        {
            foreach (var tupleTable in await ListTables(ydbConnection, tableType, cancellationToken))
            {
                await AppendDescribeTable(
                    ydbConnection: ydbConnection,
                    tableName: tupleTable.TableName,
                    tableType: tableType,
                    (ydbTable, type) =>
                    {
                        var row = table.Rows.Add();
                        var tableStats = ydbTable.TableStats!;

                        row["table_name"] = tupleTable.TableName;
                        row["table_type"] = type;
                        row["rows_estimate"] = tableStats.RowsEstimate;
                        row["creation_time"] = tableStats.CreationTime;
                        row["modification_time"] = (object?)tableStats.ModificationTime ?? DBNull.Value;
                    },
                    new DescribeTableSettings { IncludeTableStats = true },
                    cancellationToken
                );
            }
        }
        else
        {
            await AppendDescribeTable(
                ydbConnection: ydbConnection,
                tableName: tableName,
                tableType: tableType,
                (ydbTable, type) =>
                {
                    var row = table.Rows.Add();
                    var tableStats = ydbTable.TableStats!;

                    row["table_name"] = tableName;
                    row["table_type"] = type;
                    row["rows_estimate"] = tableStats.RowsEstimate;
                    row["creation_time"] = tableStats.CreationTime;
                    row["modification_time"] = (object?)tableStats.ModificationTime ?? DBNull.Value;
                },
                new DescribeTableSettings { IncludeTableStats = true },
                cancellationToken: cancellationToken
            );
        }

        return table;
    }

    private static async Task<DataTable> GetColumns(
        YdbConnection ydbConnection,
        string?[] restrictions,
        CancellationToken cancellationToken)
    {
        var table = new DataTable("Columns")
        {
            Locale = CultureInfo.InvariantCulture,
            Columns =
            {
                new DataColumn("table_name"),
                new DataColumn("column_name"),
                new DataColumn("ordinal_position", typeof(int)),
                new DataColumn("is_nullable"),
                new DataColumn("data_type"),
                new DataColumn("family_name")
            }
        };
        var tableNameRestriction = restrictions[0];
        var columnName = restrictions[1];

        var tableNames = await ListTableNames(ydbConnection, tableNameRestriction, cancellationToken);
        foreach (var tableName in tableNames)
        {
            await AppendDescribeTable(
                ydbConnection,
                tableName,
                null,
                (result, _) =>
                {
                    for (var ordinal = 0; ordinal < result.Columns.Count; ordinal++)
                    {
                        var column = result.Columns[ordinal];

                        if (!column.Name.IsPattern(columnName))
                        {
                            continue;
                        }

                        var row = table.Rows.Add();

                        row["table_name"] = tableName;
                        row["column_name"] = column.Name;
                        row["ordinal_position"] = ordinal;
                        row["is_nullable"] = column.IsNullable ? "YES" : "NO";
                        row["data_type"] = column.StorageType.ToString();
                        row["family_name"] = column.Family;
                    }
                }, cancellationToken: cancellationToken);
        }

        return table;
    }

    private static async Task AppendDescribeTable(
        YdbConnection ydbConnection,
        string tableName,
        string? tableType,
        Action<YdbTableDescription, string> appendInTable,
        DescribeTableSettings settings = default,
        CancellationToken cancellationToken = default)
    {
        var ydbTable = await DescribeTable(ydbConnection.Session.Driver, tableName, settings, cancellationToken);
        var type = ydbTable.IsSystem
            ? "SYSTEM_TABLE"
            : ydbTable.Type switch
            {
                YdbTableType.Raw => "TABLE",
                YdbTableType.Column => "COLUMN_TABLE",
                YdbTableType.External => "EXTERNAL_TABLE",
                _ => throw new ArgumentOutOfRangeException(nameof(tableType))
            };
        if (type.IsPattern(tableType))
        {
            appendInTable(ydbTable, type);
        }
    }

    private static async Task<IEnumerable<string>> ListTableNames(
        YdbConnection ydbConnection,
        string? tableName,
        CancellationToken cancellationToken
    ) => tableName != null
        ? new List<string> { tableName }
        : from table in await ListTables(ydbConnection, cancellationToken: cancellationToken)
        select table.TableName;

    private static async Task<IEnumerable<(string TableName, string TableType)>> ListTables(
        YdbConnection ydbConnection,
        string? tableType = null,
        CancellationToken cancellationToken = default
    ) => from ydbObject in await SchemaObjects(ydbConnection, cancellationToken)
        let type = ydbObject.IsSystem
            ? "SYSTEM_TABLE"
            : ydbObject.Type switch
            {
                SchemeType.Table => "TABLE",
                SchemeType.ColumnTable => "COLUMN_TABLE",
                SchemeType.ExternalTable => "EXTERNAL_TABLE",
                _ => null
            }
        where type != null && type.IsPattern(tableType)
        select (ydbObject.Name, type);

    private static async Task<DataTable> GetDataSourceInformation(YdbConnection ydbConnection)
    {
        var ydbVersion =
            (await new YdbCommand(ydbConnection) { CommandText = "SELECT Version();" }.ExecuteScalarAsync())!
            .ToString();

        var table = new DataTable("DataSourceInformation");
        var row = table.Rows.Add();

        table.Columns.Add("CompositeIdentifierSeparatorPattern", typeof(string));
        table.Columns.Add("DataSourceProductName", typeof(string));
        table.Columns.Add("DataSourceProductVersion", typeof(string));
        table.Columns.Add("DataSourceProductVersionNormalized", typeof(string));
        table.Columns.Add("GroupByBehavior", typeof(GroupByBehavior));
        table.Columns.Add("IdentifierPattern", typeof(string));
        table.Columns.Add("IdentifierCase", typeof(IdentifierCase));
        table.Columns.Add("OrderByColumnsInSelect", typeof(bool));
        table.Columns.Add("ParameterMarkerFormat", typeof(string));
        table.Columns.Add("ParameterMarkerPattern", typeof(string));
        table.Columns.Add("ParameterNameMaxLength", typeof(int));
        table.Columns.Add("QuotedIdentifierPattern", typeof(string));
        table.Columns.Add("QuotedIdentifierCase", typeof(IdentifierCase));
        table.Columns.Add("ParameterNamePattern", typeof(string));
        table.Columns.Add("StatementSeparatorPattern", typeof(string));
        table.Columns.Add("StringLiteralPattern", typeof(string));
        table.Columns.Add("SupportedJoinOperators", typeof(SupportedJoinOperators));

        row["CompositeIdentifierSeparatorPattern"] = "\\/";
        row["DataSourceProductName"] = "YDB";
        row["DataSourceProductVersion"] = row["DataSourceProductVersionNormalized"] = ydbVersion;
        row["GroupByBehavior"] = GroupByBehavior.Unrelated;
        row["IdentifierPattern"] = // copy-paste from MySQL and PostgreSQL
            @"(^\`\p{Lo}\p{Lu}\p{Ll}_@#][\p{Lo}\p{Lu}\p{Ll}\p{Nd}@$#_]*$)|(^\`[^\`\0]|\`\`+\`$)|(^\"" + [^\""\0]|\""\""+\""$)";
        row["IdentifierCase"] = IdentifierCase.Insensitive;
        row["OrderByColumnsInSelect"] = false;
        row["QuotedIdentifierPattern"] = @"(([^\`]|\`\`)*)";
        row["QuotedIdentifierCase"] = IdentifierCase.Sensitive;
        row["StatementSeparatorPattern"] = ";";
        row["StringLiteralPattern"] = "'(([^']|'')*)'|'(([^\"]|\"\")*)'";
        row["SupportedJoinOperators"] =
            SupportedJoinOperators.FullOuter |
            SupportedJoinOperators.Inner |
            SupportedJoinOperators.LeftOuter |
            SupportedJoinOperators.RightOuter;

        row["ParameterMarkerFormat"] = "{0}";
        row["ParameterMarkerPattern"] = "(@[A-Za-z0-9_$#]*)";
        row["ParameterNameMaxLength"] = int.MaxValue;
        row["ParameterNamePattern"] =
            @"^[\p{Lo}\p{Lu}\p{Ll}\p{Lm}_@#][\p{Lo}\p{Lu}\p{Ll}\p{Lm}\p{Nd}\uff3f_@#\$]*(?=\s+|$)";

        return table;
    }

    private static DataTable GetMetaDataCollections()
    {
        var table = new DataTable("MetaDataCollections");
        table.Columns.Add("CollectionName", typeof(string));
        table.Columns.Add("NumberOfRestrictions", typeof(int));
        table.Columns.Add("NumberOfIdentifierParts", typeof(int));

        // Common Schema Collections
        table.Rows.Add("MetaDataCollections", 0, 0);
        table.Rows.Add("DataSourceInformation", 0, 0);
        table.Rows.Add("Restrictions", 0, 0);
        table.Rows.Add("DataTypes", 0, 0);
        table.Rows.Add("ReservedWords", 0, 0);

        // Ydb Specific Schema Collections
        table.Rows.Add("Tables", 2, 1);
        table.Rows.Add("TablesWithStats", 2, 1);
        table.Rows.Add("Columns", 2, 2);

        return table;
    }

    private static DataTable GetRestrictions()
    {
        var table = new DataTable("Restrictions");

        table.Columns.Add("CollectionName", typeof(string));
        table.Columns.Add("RestrictionName", typeof(string));
        table.Columns.Add("RestrictionDefault", typeof(string));
        table.Columns.Add("RestrictionNumber", typeof(int));

        table.Rows.Add("Tables", "Table", "TABLE_NAME", 1);
        table.Rows.Add("Tables", "TableType", "TABLE_TYPE", 2);
        table.Rows.Add("TablesWithStats", "Table", "TABLE_NAME", 1);
        table.Rows.Add("TablesWithStats", "TableType", "TABLE_TYPE", 2);
        table.Rows.Add("Columns", "Table", "TABLE_NAME", 1);
        table.Rows.Add("Columns", "Column", "COLUMN_NAME", 2);

        return table;
    }

    private static async Task<IReadOnlyCollection<YdbObject>> SchemaObjects(
        YdbConnection ydbConnection,
        string databasePath,
        string path,
        CancellationToken cancellationToken
    )
    {
        try
        {
            var fullPath = WithSuffix(path);
            var ydbSchemaObjects = new List<YdbObject>();
            var response = await ydbConnection.Session.Driver.UnaryCall(
                SchemeService.ListDirectoryMethod,
                new ListDirectoryRequest { Path = fullPath },
                new GrpcRequestSettings { CancellationToken = cancellationToken }
            );

            var operation = response.Operation;
            if (operation.Status.IsNotSuccess())
            {
                throw YdbException.FromServer(operation.Status, operation.Issues);
            }

            foreach (var entry in operation.Result.Unpack<ListDirectoryResult>().Children)
            {
                var ydbObjectPath = fullPath[databasePath.Length..] + entry.Name;


                switch (entry.Type)
                {
                    case Entry.Types.Type.Directory:
                        ydbSchemaObjects.AddRange(
                            await SchemaObjects(
                                ydbConnection,
                                databasePath,
                                fullPath + entry.Name,
                                cancellationToken
                            )
                        );
                        break;
                    case Entry.Types.Type.Table:
                    case Entry.Types.Type.ColumnTable:
                    case Entry.Types.Type.Unspecified:
                    case Entry.Types.Type.PersQueueGroup:
                    case Entry.Types.Type.Database:
                    case Entry.Types.Type.RtmrVolume:
                    case Entry.Types.Type.BlockStoreVolume:
                    case Entry.Types.Type.CoordinationNode:
                    case Entry.Types.Type.ColumnStore:
                    case Entry.Types.Type.Sequence:
                    case Entry.Types.Type.Replication:
                    case Entry.Types.Type.Topic:
                    case Entry.Types.Type.ExternalTable:
                    case Entry.Types.Type.ExternalDataSource:
                    case Entry.Types.Type.View:
                        ydbSchemaObjects.Add(new YdbObject(entry.Type, ydbObjectPath));
                        break;
                    default:
                        continue;
                }
            }

            return ydbSchemaObjects;
        }
        catch (YdbException e)
        {
            ydbConnection.OnNotSuccessStatusCode(e.Code);

            throw;
        }
    }

    private static string WithSuffix(string path) => path.EndsWith('/') ? path : path + '/';

    private static bool IsPattern(this string tableType, string? expectedTableType) =>
        expectedTableType == null || expectedTableType.Equals(tableType, StringComparison.OrdinalIgnoreCase);
}
