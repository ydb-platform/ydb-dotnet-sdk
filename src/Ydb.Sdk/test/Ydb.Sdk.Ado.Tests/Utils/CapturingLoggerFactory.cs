using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Ado.Tests.Utils;

internal sealed class CapturingLoggerFactory : ILoggerFactory
{
    public List<(LogLevel Level, string Message, Exception? Error)> Entries { get; } = [];

    public ILogger CreateLogger(string categoryName) => new CapturingLogger(this);

    public void AddProvider(ILoggerProvider provider)
    {
    }

    public void Dispose()
    {
    }

    private sealed class CapturingLogger(CapturingLoggerFactory factory) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            factory.Entries.Add((logLevel, formatter(state, exception), exception));
    }
}
