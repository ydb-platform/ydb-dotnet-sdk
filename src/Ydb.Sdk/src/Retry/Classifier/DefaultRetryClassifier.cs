using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Grpc.Core;

namespace Ydb.Sdk.Retry.Classifier;

internal sealed class DefaultRetryClassifier : IRetryClassifier
{
    public static readonly DefaultRetryClassifier Instance = new();

    private static readonly string[] MemberNames = { "StatusCode", "Status", "YdbStatusCode", "Code" };

    private sealed class Accessors
    {
        public Func<object, int?>? ReadDirect { get; init; }
        public Func<object, object?>? ReadResponse { get; init; }
    }

    private static readonly ConcurrentDictionary<global::System.Type, Accessors> Cache = new();

    [UnconditionalSuppressMessage("Trimming", "IL2057")]
    [UnconditionalSuppressMessage("AOT", "IL3050")]
    public Failure? Classify(Exception ex)
    {
        if (ex is RpcException rx)
            return new Failure(rx, null, (int)rx.StatusCode);

        var f = TryClassify(ex);
        if (f is not null) return f;

        if (ex is AggregateException aggr)
        {
            foreach (var inner in aggr.Flatten().InnerExceptions)
            {
                if (inner is RpcException irx)
                    return new Failure(ex, null, (int)irx.StatusCode);
                f = TryClassify(inner);
                if (f is not null) return f;
            }
        }

        var cur = ex.InnerException;
        while (cur is not null)
        {
            if (cur is RpcException rx2)
                return new Failure(ex, null, (int)rx2.StatusCode);
            f = TryClassify(cur);
            if (f is not null) return f;
            cur = cur.InnerException;
        }

        return new Failure(ex);
    }

    private static Accessors Build(global::System.Type t)
    {
        static int? ReadIntOrEnum(object? val, global::System.Type type)
        {
            if (val is null) return null;
            if (val is int i) return i;
            if (type.IsEnum) return Convert.ToInt32(val);
            return null;
        }

        Func<object, object?> readResp = obj =>
        {
            try
            {
                return t.GetProperty("Result",
                           System.Reflection.BindingFlags.Instance |
                           System.Reflection.BindingFlags.Public |
                           System.Reflection.BindingFlags.NonPublic)?.GetValue(obj)
                       ?? t.GetProperty("Response",
                           System.Reflection.BindingFlags.Instance |
                           System.Reflection.BindingFlags.Public |
                           System.Reflection.BindingFlags.NonPublic)?.GetValue(obj);
            }
            catch { return null; }
        };

        return new Accessors { ReadDirect = Direct, ReadResponse = readResp };

        int? Direct(object obj)
        {
            try
            {
                foreach (var name in MemberNames)
                {
                    var p = t.GetProperty(name, System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic);
                    if (p is not null)
                    {
                        object? v = null;
                        try
                        {
                            v = p.GetValue(obj);
                        }
                        catch
                        {
                            /* ignore */
                        }

                        var r = ReadIntOrEnum(v, p.PropertyType);
                        if (r.HasValue) return r;
                    }

                    var f = t.GetField(name, System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic);
                    if (f is not null)
                    {
                        object? v = null;
                        try
                        {
                            v = f.GetValue(obj);
                        }
                        catch
                        {
                            /* ignore */
                        }

                        var r = ReadIntOrEnum(v, f.FieldType);
                        if (r.HasValue) return r;
                    }
                }
            }
            catch
            {
                /* ignore */
            }

            return null;
        }
    }

    private static Failure? TryClassify(Exception ex)
    {
        if (ex is RpcException rx)
            return new Failure(rx, null, (int)rx.StatusCode);

        global::System.Type t = ex.GetType();
        var acc = Cache.GetOrAdd(t, tt => Build(tt));

        var code = acc.ReadDirect?.Invoke(ex);
        if (code.HasValue) return new Failure(ex, code, null);

        var resp = acc.ReadResponse?.Invoke(ex);
        if (resp is not null)
        {
            global::System.Type rt = resp.GetType();
            var racc = Cache.GetOrAdd(rt, Build);
            var code2 = racc.ReadDirect?.Invoke(resp);
            if (code2.HasValue) return new Failure(ex, code2, null);
        }

        return null;
    }
}
