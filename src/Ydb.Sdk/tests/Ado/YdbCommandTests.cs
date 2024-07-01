using System.Data;
using Xunit;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Tests.Ado;

[Trait("Category", "Integration")]
public class YdbCommandTests
{
    [Theory]
    [ClassData(typeof(YdbParameterTests.TestDataGenerator))]
    public async Task ExecuteScalarAsync_WhenSetYdbParameter_ReturnThisValue<T>(YdbParameterTests.Data<T> data)
    {
        await using var connection = new YdbConnection();
        await connection.OpenAsync();

        var dbCommand = connection.CreateCommand();

        dbCommand.CommandText = "SELECT $var;";

        var dbParameter = new YdbParameter
        {
            ParameterName = "$var",
            DbType = data.DbType,
            Value = data.Expected,
            IsNullable = data.IsNullable
        };

        dbCommand.Parameters.Add(dbParameter);

        Assert.Equal(data.Expected, await dbCommand.ExecuteScalarAsync());
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_WhenCreateTopic_ReturnEmptyResultSet()
    {
        await using var connection = new YdbConnection();
        await connection.OpenAsync();

        var dbCommand = connection.CreateCommand();
        dbCommand.CommandText = "CREATE USER user PASSWORD '123qweqwe'";

        await dbCommand.ExecuteNonQueryAsync();

        dbCommand.CommandText = "DROP USER user;";
        await dbCommand.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task ExecuteDbDataReader_WhenSelectManyResultSet_ReturnYdbDataReader()
    {
        await using var connection = new YdbConnection();
        await connection.OpenAsync();

        var dbCommand = connection.CreateCommand();
        dbCommand.CommandText = @"
SELECT 1, CAST('text' AS Text);

$data = ListReplicate(AsStruct(true AS bool_field, 'hello' AS text_field, 1.5 AS double_field, 23 AS int_field), 1500);

SELECT bool_field, text_field, double_field, int_field  FROM AS_TABLE($data);

SELECT CAST(NULL AS Int8) AS null_byte;

$new_data = AsList(
    AsStruct($var1 AS Key, $var2 AS Value),
    AsStruct($var1 AS Key, $var2 AS Value),
    AsStruct($var1 AS Key, $var2 AS Value)
);

SELECT Key, Value FROM AS_TABLE($new_data);
";

        dbCommand.Parameters.Add(new YdbParameter("$var1", DbType.DateTime2, new DateTime(2021, 08, 21, 23, 30, 47)));
        dbCommand.Parameters.Add(new YdbParameter("$var2", DbType.DateTime2, DateTime.Parse("2029-08-03T06:59:44.8578730Z")));
        
        var ydbDataReader = await dbCommand.ExecuteReaderAsync();
        
        Assert.True(await ydbDataReader.ReadAsync());
        Assert.Equal(1, ydbDataReader.GetInt32(0));
        Assert.Equal("text", ydbDataReader.GetString(1));
        
        
    }
}
