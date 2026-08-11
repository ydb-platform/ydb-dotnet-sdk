using System.Diagnostics;
using Ydb.Sdk.Internal;

namespace Ydb.Sdk.Topic.Tracing;

internal static class YdbTopicActivitySource
{
    private static readonly ActivitySource Instance = new("Ydb.Sdk.Topic", YdbSdkVersion.Value);

    internal static bool HasListeners => Instance.HasListeners();

    internal static Activity? StartActivity(string spanName, ActivityKind activityKind) =>
        Instance.StartActivity(spanName, activityKind);
}
