using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.Ydb.Query.Expressions.Internal;


public class YdbILikeExpression(SqlExpression match,
    SqlExpression pattern, // expression
    SqlExpression? escapeChar, // escape char - optional
    RelationalTypeMapping? typeMapping)
    : SqlExpression(typeof(bool), typeMapping), IEquatable<YdbILikeExpression>
{
    private static ConstructorInfo? _quotingConstructor;
    public virtual SqlExpression Match { get; } = match;
    public virtual SqlExpression Pattern { get; } = pattern;
    public virtual SqlExpression? EscapeChar { get; } = escapeChar;

    protected override Expression VisitChildren(ExpressionVisitor visitor)
        => Update(
            (SqlExpression)visitor.Visit(Match),
            (SqlExpression)visitor.Visit(Pattern),
            EscapeChar is null ? null : (SqlExpression)visitor.Visit(EscapeChar));
    
    public virtual YdbILikeExpression Update(
        SqlExpression match,
        SqlExpression pattern,
        SqlExpression? escapeChar)
        => match == Match && pattern == Pattern && escapeChar == EscapeChar
            ? this
            : new YdbILikeExpression(match, pattern, escapeChar, TypeMapping);

    [Experimental("EF9100")]
    public override Expression Quote()
        => New(
            _quotingConstructor ??= typeof(YdbILikeExpression).GetConstructor(
                [typeof(SqlExpression), typeof(SqlExpression), typeof(SqlExpression), typeof(RelationalTypeMapping)])!,
            Match.Quote(),
            Pattern.Quote(),
            RelationalExpressionQuotingUtilities.QuoteOrNull(EscapeChar),
            RelationalExpressionQuotingUtilities.QuoteTypeMapping(TypeMapping));

    public override bool Equals(object? obj)
        => obj is YdbILikeExpression other && Equals(other);

    public virtual bool Equals(YdbILikeExpression? other)
        => ReferenceEquals(this, other)
            || other is not null
            && base.Equals(other)
            && Equals(Match, other.Match)
            && Equals(Pattern, other.Pattern)
            && Equals(EscapeChar, other.EscapeChar);

    public override int GetHashCode()
        => HashCode.Combine(base.GetHashCode(), Match, Pattern, EscapeChar);

    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        expressionPrinter.Visit(Match);
        expressionPrinter.Append(" ILIKE ");
        expressionPrinter.Visit(Pattern);

        if (EscapeChar is not null)
        {
            expressionPrinter.Append(" ESCAPE ");
            expressionPrinter.Visit(EscapeChar);
        }
    }

    public override string ToString()
        => $"{Match} ILIKE {Pattern}{(EscapeChar is null ? "" : $" ESCAPE {EscapeChar}")}";
}
