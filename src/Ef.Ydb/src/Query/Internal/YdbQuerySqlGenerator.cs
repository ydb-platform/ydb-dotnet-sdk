using Microsoft.EntityFrameworkCore.Query;

namespace Ef.Ydb.Query.Internal;

public class YdbQuerySqlGenerator
    : QuerySqlGenerator
{
    public YdbQuerySqlGenerator(
        QuerySqlGeneratorDependencies dependencies
    ) : base(dependencies)
    {
    }
}
