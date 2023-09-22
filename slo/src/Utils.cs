using Ydb.Sdk;

namespace slo;

public static class Utils
{
    public static string GetResonseStatusName(StatusCode statusCode)
    {
        var prefix = statusCode >= StatusCode.ClientTransportResourceExhausted ? "GRPC" : "YDB";
        return $"{prefix}_{statusCode}";
    }
}