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
        if (left is SqlFunctionExpression funcExpression
            &&
            right is SqlConstantExpression { TypeMapping: not null } constExpression
            &&
            funcExpression.Name.Equals("SUM", StringComparison.OrdinalIgnoreCase)
            &&
            funcExpression.Arguments != null
            &&
            constExpression.TypeMapping.DbType == DbType.Decimal
            &&
            constExpression.Value != null)
        {
            // get column expression for SUM function expression
            var columnExpression = funcExpression.Arguments[0] as ColumnExpression;

            var correctRight = new SqlConstantExpression(constExpression.Value,
                YdbDecimalTypeMapping.CreateMaxPrecision(columnExpression?.TypeMapping?.Scale));

            return base.Coalesce(left, correctRight, typeMapping);
        }

        return base.Coalesce(left, right, typeMapping);
    }
}
