using System;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.Ydb.Query.Expressions;

public class YdbILikeExpression: SqlExpression
{
	public const string ILikeConst = "ILIKE";
	public const string ILikeWithSpacesConst = " " + ILikeConst + " ";

	public virtual SqlExpression Match { get; }
	
	public virtual SqlExpression Pattern { get; }
	
	public YdbILikeExpression(SqlExpression match, SqlExpression pattern, RelationalTypeMapping? typeMapping)
		: base(typeof(bool), typeMapping)
	{
		Match = match;
		Pattern = pattern;
	}

	public override Expression Quote() => new YdbILikeExpression((SqlExpression)Match.Quote(), (SqlExpression)Pattern.Quote(), TypeMapping);

	protected override Expression VisitChildren(ExpressionVisitor visitor)
		=> Update(
			(SqlExpression)visitor.Visit(Match),
			(SqlExpression)visitor.Visit(Pattern));
	
	public  YdbILikeExpression Update(
		SqlExpression match,
		SqlExpression pattern)
		=> match == Match && pattern == Pattern
			? this
			: new YdbILikeExpression(match, pattern, TypeMapping);
	
	public override bool Equals(object? obj)
		=> obj is YdbILikeExpression other && Equals(other);
	
	public virtual bool Equals(YdbILikeExpression? other)
		=> ReferenceEquals(this, other)
		   || other is not null
		   && base.Equals(other)
		   && Equals(Match, other.Match)
		   && Equals(Pattern, other.Pattern);
	
	public override int GetHashCode()
		=> HashCode.Combine(base.GetHashCode(), Match, Pattern);

	protected override void Print(ExpressionPrinter expressionPrinter)
	{
		expressionPrinter.Visit(Match);
		expressionPrinter.Append(ILikeWithSpacesConst);
		expressionPrinter.Visit(Pattern);
	}

	public override string ToString() => $"{Match} ILIKE {Pattern}";
}
