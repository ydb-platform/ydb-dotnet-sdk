namespace Ydb.Sdk.Ado.Session;

internal sealed class ImplicitSessionSource : ISessionSource
{
    private readonly IDriver _driver;

    internal ImplicitSessionSource(IDriver driver)
    {
        _driver = driver;
    }

    public ValueTask<ISession> OpenSession(CancellationToken cancellationToken) => new(new ImplicitSession(_driver));

    public async ValueTask DisposeAsync() => await _driver.DisposeAsync();
}