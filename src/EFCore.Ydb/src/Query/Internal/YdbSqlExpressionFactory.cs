using System;
using System.Diagnostics.CodeAnalysis;
using EntityFrameworkCore.Ydb.Query.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.Ydb.Query.Internal;

public class YdbSqlExpressionFactory(SqlExpressionFactoryDependencies dependencies) : SqlExpressionFactory(dependencies)
{
	[return: NotNullIfNotNull("sqlExpression")]
	public override SqlExpression? ApplyTypeMapping(SqlExpression? sqlExpression, RelationalTypeMapping? typeMapping) =>
		sqlExpression switch
		{
			YdbILikeExpression ilikeExpression => ApplyILikeExpression(ilikeExpression, typeMapping),
			_ => base.ApplyTypeMapping(sqlExpression, typeMapping)
		};


	protected SqlExpression ApplyILikeExpression(YdbILikeExpression ilikeExpression, RelationalTypeMapping? typeMapping)
	{
		var inferredTypeMapping = ExpressionExtensions.InferTypeMapping(ilikeExpression.Match, ilikeExpression.Pattern);
		
		return new YdbILikeExpression(ApplyTypeMapping(ilikeExpression.Match, inferredTypeMapping),
			ApplyTypeMapping(ilikeExpression.Pattern, inferredTypeMapping), typeMapping);
	}
	
	public YdbILikeExpression ILike(
		SqlExpression match,
		SqlExpression pattern)
		=> (YdbILikeExpression)ApplyDefaultTypeMapping(new YdbILikeExpression(match, pattern, null));
}
