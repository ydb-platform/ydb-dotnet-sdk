using System;
using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;

namespace EfCore.Ydb.Storage.Internal;

public class YdbRelationalTransactionFactory : IRelationalTransactionFactory
{
    public YdbRelationalTransactionFactory(RelationalTransactionFactoryDependencies dependencies)
        => Dependencies = dependencies;

    protected virtual RelationalTransactionFactoryDependencies Dependencies { get; }

    public RelationalTransaction Create(
        IRelationalConnection connection,
        DbTransaction transaction,
        Guid transactionId,
        IDiagnosticsLogger<DbLoggerCategory.Database.Transaction> logger,
        bool transactionOwned
    ) => new YdbRelationalTransaction(
        connection,
        transaction,
        transactionId,
        logger,
        transactionOwned,
        Dependencies.SqlGenerationHelper
    );
}
