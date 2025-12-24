using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Scaffolding.Metadata;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.Schema;

namespace EntityFrameworkCore.Ydb.Scaffolding.Internal;

public class YdbDatabaseModelFactory : DatabaseModelFactory
{
    public override DatabaseModel Create(string connectionString, DatabaseModelFactoryOptions options)
    {
        using var connection = new YdbConnection(connectionString);

        return Create(connection, options);
    }

    public override DatabaseModel Create(DbConnection connection, DatabaseModelFactoryOptions options)
    {
        var ydbConnection = (YdbConnection)connection;
        var connectionStartedOpen = ydbConnection.State == ConnectionState.Open;

        if (!connectionStartedOpen)
        {
            ydbConnection.Open();
        }

        try
        {
            var tableNames = new List<string>();
            tableNames.AddRange(options.Tables);

            if (tableNames.Count == 0)
            {
                tableNames.AddRange(
                    from ydbObject in YdbSchema.SchemaObjects(ydbConnection).GetAwaiter().GetResult()
                    where ydbObject.Type is SchemeType.Table or SchemeType.ColumnTable or SchemeType.ExternalTable &&
                          !ydbObject.IsSystem
                    select ydbObject.Name
                );
            }

            var databaseModel = new DatabaseModel
            {
                DatabaseName = connection.Database
            };

            foreach (var ydbTable in tableNames.Select(tableName => YdbSchema.DescribeTable(
                         ydbConnection.Session.Driver, tableName).GetAwaiter().GetResult()))
            {
                var databaseTable = new DatabaseTable
                {
                    Name = ydbTable.Name,
                    Database = databaseModel
                };

                var columnNameToDatabaseColumn = new Dictionary<string, DatabaseColumn>();

                foreach (var column in ydbTable.Columns)
                {
                    var databaseColumn = new DatabaseColumn
                    {
                        Name = column.Name,
                        Table = databaseTable,
                        StoreType = column.StorageType.ToString(),
                        IsNullable = column.IsNullable
                    };

                    databaseTable.Columns.Add(databaseColumn);
                    columnNameToDatabaseColumn[column.Name] = databaseColumn;
                }

                foreach (var index in ydbTable.Indexes)
                {
                    var databaseIndex = new DatabaseIndex
                    {
                        Name = index.Name,
                        Table = databaseTable,
                        IsUnique = index.Type == YdbIndexType.GlobalUnique
                    };

                    foreach (var columnName in index.Columns)
                    {
                        databaseIndex.Columns.Add(columnNameToDatabaseColumn[columnName]);
                        databaseIndex.IsDescending.Add(false);
                    }

                    databaseTable.Indexes.Add(databaseIndex);
                }

                databaseTable.PrimaryKey = new DatabasePrimaryKey
                {
                    Name = null // YDB does not have a primary key named
                };

                foreach (var columnName in ydbTable.PrimaryKey)
                {
                    databaseTable.PrimaryKey.Columns.Add(columnNameToDatabaseColumn[columnName]);
                }

                databaseModel.Tables.Add(databaseTable);
            }

            return databaseModel;
        }
        finally
        {
            if (!connectionStartedOpen)
            {
                ydbConnection.Close();
            }
        }
    }
}
