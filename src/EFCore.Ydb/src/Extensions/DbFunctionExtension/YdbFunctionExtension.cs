using System;
using Microsoft.EntityFrameworkCore.Diagnostics;

// ReSharper disable once CheckNamespace
namespace Microsoft.EntityFrameworkCore;

public static class YdbFunctionExtension
{
    public static bool ILike(this DbFunctions _, string matchExpression, string pattern)
        => throw new InvalidOperationException(CoreStrings.FunctionOnClient(nameof(ILike)));
    public static bool ILike(this DbFunctions _, string matchExpression, string pattern, string escapeCharacter)
        => throw new InvalidOperationException(CoreStrings.FunctionOnClient(nameof(ILike)));
}
