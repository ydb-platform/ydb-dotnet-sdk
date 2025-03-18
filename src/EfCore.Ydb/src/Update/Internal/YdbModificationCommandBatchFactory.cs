using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Ydb.Sdk.Ado;

namespace EfCore.Ydb.Update.Internal;

public class YdbModificationCommandBatchFactory : IModificationCommandBatchFactory
{
    public YdbModificationCommandBatchFactory(ModificationCommandBatchFactoryDependencies dependencies)
        => Dependencies = dependencies;

    protected virtual ModificationCommandBatchFactoryDependencies Dependencies { get; }

    public ModificationCommandBatch Create()
        => new TemporaryStubModificationCommandBatch(Dependencies);
}

class TemporaryStubModificationCommandBatch : ReaderModificationCommandBatch
{
    public TemporaryStubModificationCommandBatch(ModificationCommandBatchFactoryDependencies dependencies)
        : base(dependencies, 2000)
    {
    }

    protected override void Consume(RelationalDataReader reader)
    {
        ConsumeAsync(reader).ConfigureAwait(false).GetAwaiter().GetResult();
    }

    protected override async Task ConsumeAsync(
        RelationalDataReader? reader,
        CancellationToken cancellationToken = default
    )
    {
        // In ideal world we want to read result of commands,
        // but right now all modification commands return nothing
        var commandIndex = 0;
        try
        {
            while (commandIndex < ModificationCommands.Count)
            {
                if (ResultSetMappings[commandIndex].HasFlag(ResultSetMapping.NoResults))
                {
                    // Skip
                }
                else
                {
                    // TODO: implement in case commands return type will change 
                }

                commandIndex++;
            }
        }
        catch (Exception ex) when (ex is not DbUpdateException and not OperationCanceledException)
        {
            if (commandIndex == ModificationCommands.Count)
            {
                commandIndex--;
            }

            throw new DbUpdateException(
                RelationalStrings.UpdateStoreException,
                ex,
                ModificationCommands[commandIndex].Entries
            );
        }
    }

    protected override void AddCommand(IReadOnlyModificationCommand modificationCommand)
    {
        bool requiresTransaction;
        var commandPosition = ResultSetMappings.Count;

        switch (modificationCommand.EntityState)
        {
            case EntityState.Added:
                UpdateSqlGenerator.AppendInsertOperation(
                    SqlBuilder, modificationCommand, commandPosition, out requiresTransaction);
                break;
            case EntityState.Modified:
                UpdateSqlGenerator.AppendUpdateOperation(
                    SqlBuilder, modificationCommand, commandPosition, out requiresTransaction);
                break;
            case EntityState.Deleted:
                UpdateSqlGenerator.AppendDeleteOperation(
                    SqlBuilder, modificationCommand, commandPosition, out requiresTransaction);
                break;
            default:
                throw new InvalidOperationException(
                    RelationalStrings.ModificationCommandInvalidEntityState(
                        modificationCommand.Entries[0].EntityType,
                        modificationCommand.EntityState)
                    );
        }

        ResultSetMappings.Add(ResultSetMapping.NoResults);

        AddParameters(modificationCommand);
        SetRequiresTransaction(commandPosition > 0 || requiresTransaction);
    }
}
