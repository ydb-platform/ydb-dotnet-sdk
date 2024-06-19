namespace Ydb.Sdk.GrpcWrappers.Topic.ControlPlane;

public enum MeteringMode
{
    Unspecified = Ydb.Topic.MeteringMode.Unspecified,
    ReservedCapacity = Ydb.Topic.MeteringMode.ReservedCapacity,
    RequestUnits = Ydb.Topic.MeteringMode.RequestUnits
}
