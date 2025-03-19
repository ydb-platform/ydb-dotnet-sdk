using System.Collections.Immutable;
using System.Data;
using System.Data.Common;
using System.Globalization;
using Ydb.Scheme;
using Ydb.Scheme.V1;
using Ydb.Sdk.Services.Table;
using Ydb.Table;

namespace Ydb.Sdk.Ado;

internal static class YdbSchema
{
    public static Task<DataTable> GetSchemaAsync(
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
        var database = ydbConnection.Database;

        if (tableName == null) // tableName isn't set
        {
            foreach (var tupleTable in
                     await ListTables(ydbConnection, WithSuffix(database), database, tableType, cancellationToken))
            {
                table.Rows.Add(tupleTable.TableName, tupleTable.TableType);
            }
        }
        else
        {
            await AppendDescribeTable(
                ydbConnection: ydbConnection,
                describeTableSettings: new DescribeTableSettings { CancellationToken = cancellationToken },
                tableName: tableName,
                tableType: tableType,
                (_, type) => { table.Rows.Add(tableName, type); });
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
        var database = ydbConnection.Database;

        if (tableName == null) // tableName isn't set
        {
            foreach (var tupleTable in
                     await ListTables(ydbConnection, WithSuffix(database), database, tableType, cancellationToken))
            {
                await AppendDescribeTable(
                    ydbConnection: ydbConnection,
                    describeTableSettings: new DescribeTableSettings { CancellationToken = cancellationToken }
                        .WithTableStats(),
                    tableName: tupleTable.TableName,
                    tableType: tableType,
                    (describeTableResult, type) =>
                    {
                        var row = table.Rows.Add();
                        var tableStats = describeTableResult.TableStats;

                        row["table_name"] = tupleTable.TableName;
                        row["table_type"] = type;
                        row["rows_estimate"] = tableStats.RowsEstimate;
                        row["creation_time"] = tableStats.CreationTime.ToDateTime();
                        row["modification_time"] = (object?)tableStats.ModificationTime?.ToDateTime() ?? DBNull.Value;
                    });
            }
        }
        else
        {
            await AppendDescribeTable(
                ydbConnection: ydbConnection,
                describeTableSettings: new DescribeTableSettings { CancellationToken = cancellationToken }
                    .WithTableStats(),
                tableName: tableName,
                tableType: tableType,
                (describeTableResult, type) =>
                {
                    var row = table.Rows.Add();
                    var tableStats = describeTableResult.TableStats;

                    row["table_name"] = tableName;
                    row["table_type"] = type;
                    row["rows_estimate"] = tableStats.RowsEstimate;
                    row["creation_time"] = tableStats.CreationTime.ToDateTime();
                    row["modification_time"] = (object?)tableStats.ModificationTime?.ToDateTime() ?? DBNull.Value;
                });
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
                new DescribeTableSettings { CancellationToken = cancellationToken },
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
                        var type = column.Type;

                        row["table_name"] = tableName;
                        row["column_name"] = column.Name;
                        row["ordinal_position"] = ordinal;
                        row["is_nullable"] = type.TypeCase == Type.TypeOneofCase.OptionalType ? "YES" : "NO";
                        row["data_type"] = type.YqlTableType();
                        row["family_name"] = column.Family;
                    }
                }
            );
        }

        return table;
    }

    private static async Task AppendDescribeTable(
        YdbConnection ydbConnection,
        DescribeTableSettings describeTableSettings,
        string tableName,
        string? tableType,
        Action<DescribeTableResult, string> appendInTable)
    {
        try
        {
            var describeResponse = await ydbConnection.Session
                .DescribeTable(WithSuffix(ydbConnection.Database) + tableName, describeTableSettings);

            if (describeResponse.Operation.Status == StatusIds.Types.StatusCode.SchemeError)
            {
                // ignore scheme errors like path not found
                return;
            }

            var status = Status.FromProto(describeResponse.Operation.Status, describeResponse.Operation.Issues);

            if (status.IsNotSuccess)
            {
                ydbConnection.Session.OnStatus(status);

                throw new YdbException(status);
            }

            var describeRes = describeResponse.Operation.Result.Unpack<DescribeTableResult>();

            // ReSharper disable once SwitchExpressionHandlesSomeKnownEnumValuesWithExceptionInDefault
            var type = describeRes.Self.Type switch
            {
                Entry.Types.Type.Table => tableName.IsSystem() ? "SYSTEM_TABLE" : "TABLE",
                Entry.Types.Type.ColumnTable => "COLUMN_TABLE",
                _ => throw new YdbException($"Unexpected entry type for Table: {describeRes.Self.Type}")
            };

            if (type.IsPattern(tableType))
            {
                appendInTable(describeRes, type);
            }
        }
        catch (Driver.TransportException e)
        {
            ydbConnection.Session.OnStatus(e.Status);

            throw new YdbException("Transport error on DescribeTable", e);
        }
    }

    private static async Task<IReadOnlyCollection<string>> ListTableNames(
        YdbConnection ydbConnection,
        string? tableName,
        CancellationToken cancellationToken)
    {
        var database = ydbConnection.Database;

        return tableName != null
            ? new List<string> { tableName }
            : (await ListTables(
                ydbConnection,
                WithSuffix(database),
                database,
                null,
                cancellationToken
            )).Select(tuple => tuple.TableName).ToImmutableList();
    }

    private static async Task<IReadOnlyCollection<(string TableName, string TableType)>> ListTables(
        YdbConnection ydbConnection,
        string databasePath,
        string path,
        string? tableType,
        CancellationToken cancellationToken)
    {
        try
        {
            var fullPath = WithSuffix(path);
            var tables = new List<(string, string)>();
            var response = await ydbConnection.Session.Driver.UnaryCall(
                SchemeService.ListDirectoryMethod,
                new ListDirectoryRequest { Path = fullPath },
                new GrpcRequestSettings { CancellationToken = cancellationToken }
            );

            var operation = response.Operation;
            var status = Status.FromProto(operation.Status, operation.Issues);

            if (status.IsNotSuccess)
            {
                throw new YdbException(status);
            }

            foreach (var entry in operation.Result.Unpack<ListDirectoryResult>().Children)
            {
                var tablePath = fullPath[databasePath.Length..] + entry.Name;

                switch (entry.Type)
                {
                    case Entry.Types.Type.Table:
                        var type = tablePath.IsSystem() ? "SYSTEM_TABLE" : "TABLE";
                        if (type.IsPattern(tableType))
                        {
                            tables.Add((tablePath, type));
                        }

                        break;
                    case Entry.Types.Type.ColumnTable:
                        if ("COLUMN_TABLE".IsPattern(tableType))
                        {
                            tables.Add((tablePath, "COLUMN_TABLE"));
                        }

                        break;
                    case Entry.Types.Type.Directory:
                        tables.AddRange(
                            await ListTables(ydbConnection, databasePath, fullPath + entry.Name, tableType,
                                cancellationToken)
                        );
                        break;
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
                    default:
                        continue;
                }
            }

            return tables;
        }
        catch (Driver.TransportException e)
        {
            throw new YdbException("Transport error on ListDirectory", e);
        }
    }

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
        ;
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

    private static string WithSuffix(string path) => path.EndsWith('/') ? path : path + '/';

    private static bool IsSystem(this string tablePath) => tablePath.StartsWith(".sys/")
                                                           || tablePath.StartsWith(".sys_health/")
                                                           || tablePath.StartsWith(".sys_health_dev/");

    private static bool IsPattern(this string tableType, string? expectedTableType) =>
        expectedTableType == null || expectedTableType.Equals(tableType, StringComparison.OrdinalIgnoreCase);

    internal static string YqlTableType(this Type type) => type.TypeCase == Type.TypeOneofCase.OptionalType
        ? type.OptionalType.Item.TypeId.ToString()
        : type.TypeId.ToString();
}
