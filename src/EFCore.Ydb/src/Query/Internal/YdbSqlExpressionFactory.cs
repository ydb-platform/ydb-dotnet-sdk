using System;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using EntityFrameworkCore.Ydb.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
namespace EntityFrameworkCore.Ydb.Query.Internal;

public class YdbSqlExpressionFactory(SqlExpressionFactoryDependencies dependencies) : SqlExpressionFactory(dependencies)
{
    [return: NotNullIfNotNull("sqlExpression")]
    public override SqlExpression? ApplyTypeMapping(SqlExpression? sqlExpression, RelationalTypeMapping? typeMapping) =>
        base.ApplyTypeMapping(sqlExpression, typeMapping);

    public override SqlExpression Coalesce(SqlExpression left, SqlExpression right,
        RelationalTypeMapping? typeMapping = null)
    {
        // For .Sum(x => x.Decimal) EF generates coalesce(sum(x.Decimal), 0.0)) because SUM must have value
        var funcExpression = left as SqlFunctionExpression;
        var constExpression = right as SqlConstantExpression;
        
        if (funcExpression != null && constExpression != null && constExpression.TypeMapping != null
            &&
            funcExpression.Name.Equals("SUM", StringComparison.OrdinalIgnoreCase)
            &&
            constExpression.TypeMapping.DbType == DbType.Decimal
            &&
            constExpression.Value != null)
        {
            var correctRight = new SqlConstantExpression(constExpression.Value,
                YdbDecimalTypeMapping.WithMaxPrecision); // in the feature change static max precision/scale to
                                                         // to dynamically created correct precision/scale
                                                         // it depends on db scheme and can not correctly define only in code
            
            return base.Coalesce(left, correctRight, typeMapping);
        }
        
        return base.Coalesce(left, right, typeMapping);
    }
}
