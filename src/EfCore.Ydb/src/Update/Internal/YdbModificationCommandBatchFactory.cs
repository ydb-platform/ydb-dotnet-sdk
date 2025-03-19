using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;

namespace EfCore.Ydb.Update.Internal;

public sealed class YdbModificationCommandBatchFactory(ModificationCommandBatchFactoryDependencies dependencies)
    : IModificationCommandBatchFactory
{
    private ModificationCommandBatchFactoryDependencies Dependencies { get; } = dependencies;

    public ModificationCommandBatch Create()
        => new TemporaryStubModificationCommandBatch(Dependencies);
}

internal class TemporaryStubModificationCommandBatch(ModificationCommandBatchFactoryDependencies dependencies)
    : ReaderModificationCommandBatch(dependencies, 2000)
{
    protected override void Consume(RelationalDataReader reader) =>
        ConsumeAsync(reader).ConfigureAwait(false).GetAwaiter().GetResult();

    protected override Task ConsumeAsync(
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

                // TODO: implement in case commands return type will change 
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

        return Task.CompletedTask;
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
