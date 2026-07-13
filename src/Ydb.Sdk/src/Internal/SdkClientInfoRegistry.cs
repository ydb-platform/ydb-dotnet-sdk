namespace Ydb.Sdk.Internal;

internal static class SdkClientInfoRegistry
{
    private static readonly object Lock = new();
    private static readonly SortedDictionary<string, int> Components = new(StringComparer.Ordinal);

    private static string? _chain;

    internal static void Register(string component)
    {
        if (string.IsNullOrWhiteSpace(component))
        {
            return;
        }

        lock (Lock)
        {
            Components[component] = Components.GetValueOrDefault(component) + 1;
            _chain = string.Join(';', Components.Keys);
        }
    }

    internal static void Unregister(string component)
    {
        if (string.IsNullOrWhiteSpace(component))
        {
            return;
        }

        lock (Lock)
        {
            if (!Components.TryGetValue(component, out var count))
            {
                return;
            }

            if (count <= 1)
            {
                Components.Remove(component);
            }
            else
            {
                Components[component] = count - 1;
            }

            _chain = Components.Count == 0 ? null : string.Join(';', Components.Keys);
        }
    }

    internal static string? Chain
    {
        get
        {
            lock (Lock)
            {
                return _chain;
            }
        }
    }

    internal static void Reset()
    {
        lock (Lock)
        {
            Components.Clear();
            _chain = null;
        }
    }
}
