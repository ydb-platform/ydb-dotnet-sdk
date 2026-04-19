namespace Ydb.Sdk.Coordination.DtoElection;

public class Leadership : IAsyncDisposable
{
    private readonly Semaphore _semaphore;
    private readonly Lease _lease;
    private bool _resigned;

    public Leadership(Semaphore semaphore, Lease lease)
    {
        _semaphore = semaphore;
        _lease = lease;
    }

    public async Task Proclaim(byte[]? data)
    {
        Console.WriteLine($"proclaiming leadership on {_semaphore.Name} ({data?.Length ?? 0} bytes)");

        await _semaphore.Update(data);
    }

    public async Task Resign(CancellationToken cancellationToken = default)
    {
        if (_resigned)
            return;

        _resigned = true;

        Console.WriteLine($"resigning from leadership on {_semaphore.Name}");

        await _lease.Release(cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await Resign();
        }
        finally
        {
            GC.SuppressFinalize(this);
        }
    }
}
