using Microsoft.EntityFrameworkCore.Query;

namespace EfCore.Ydb.Query.Internal;

public class YdbQuerySqlGenerator
    : QuerySqlGenerator
{
    public YdbQuerySqlGenerator(
        QuerySqlGeneratorDependencies dependencies
    ) : base(dependencies)
    {
    }
}
