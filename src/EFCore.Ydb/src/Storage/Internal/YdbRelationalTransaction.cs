using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.Ydb.Storage.Internal;

public class YdbRelationalTransaction(
    IRelationalConnection connection,
    DbTransaction transaction,
    Guid transactionId,
    IDiagnosticsLogger<DbLoggerCategory.Database.Transaction> logger,
    bool transactionOwned,
    ISqlGenerationHelper sqlGenerationHelper
) : RelationalTransaction(connection, transaction, transactionId, logger, transactionOwned, sqlGenerationHelper)
{
    public override bool SupportsSavepoints
        => false;

    public override void CreateSavepoint(string name)
        => throw new NotSupportedException("Savepoints are not supported in YDB");

    public override Task CreateSavepointAsync(string name, CancellationToken cancellationToken = new())
        => throw new NotSupportedException("Savepoints are not supported in YDB");

    public override void RollbackToSavepoint(string name)
        => throw new NotSupportedException("Savepoints are not supported in YDB");

    public override Task RollbackToSavepointAsync(string name, CancellationToken cancellationToken = new())
        => throw new NotSupportedException("Savepoints are not supported in YDB");

    public override void ReleaseSavepoint(string name)
        => throw new NotSupportedException("Savepoints are not supported in YDB");

    public override Task ReleaseSavepointAsync(string name, CancellationToken cancellationToken = new())
        => throw new NotSupportedException("Savepoints are not supported in YDB");
}
