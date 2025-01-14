using System.Data.Common;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace Ef.Ydb.Diagnostics.Internal;

// TODO: Temporary for debugging
public class YdbCommandInterceptor : DbCommandInterceptor
{
    public override InterceptionResult<DbDataReader> ReaderExecuting(
        DbCommand command,
        CommandEventData eventData,
        InterceptionResult<DbDataReader> result
    )
    {
        return result;
    }
}
