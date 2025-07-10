using System.Collections.ObjectModel;
using System.Data;
using AdoNet.Specification.Tests;

namespace Ydb.Sdk.Ado.Specification.Tests;

public class YdbSelectValueFixture : YdbFactoryFixture, ISelectValueFixture, IDeleteFixture
{
    public string CreateSelectSql(DbType dbType, ValueKind kind) =>
        $"SELECT `{dbType}` FROM `select_value` WHERE Id = {(int)kind}";

    public string CreateSelectSql(byte[] value) =>
        $"SELECT String::HexDecode('{BitConverter.ToString(value).Replace("-", string.Empty)}');";

    public IReadOnlyCollection<DbType> SupportedDbTypes => new ReadOnlyCollection<DbType>(new[]
    {
        DbType.Binary,
        DbType.Boolean,
        DbType.Byte,
        // DbType.Date,
        // DbType.DateTime,
        // DbType.Decimal,
        DbType.Double,
        DbType.Guid,
        DbType.Int16,
        DbType.Int32,
        DbType.Int64,
        DbType.SByte,
        DbType.Single,
        DbType.String,
        // DbType.DateTime2, 
        DbType.UInt16,
        DbType.UInt32,
        DbType.UInt64
    });


    public string SelectNoRows => $"SELECT 1 FROM `select_value` WHERE 0 = 1;";

    public System.Type NullValueExceptionType => typeof(InvalidCastException);

    public string DeleteNoRows => $"DELETE FROM `select_value` WHERE 0 = 1;";
}
