using System;
using Microsoft.EntityFrameworkCore;

namespace EntityFrameworkCore.Ydb.Extensions;

public static class YdbDbFunctionsExtension
{
	public const string InvalidCallMessage = "This function is designed only for LINQ queries";
	public static bool ILike(this DbFunctions dbFunctions, string match, string pattern)
		=> throw new NotSupportedException(InvalidCallMessage);
}
