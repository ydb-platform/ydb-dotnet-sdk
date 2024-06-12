namespace Ydb.Sdk.Services.Topic.Models;

public enum MeteringMode
{
    Unspecified = GrpcWrappers.Topic.ControlPlane.MeteringMode.Unspecified,
    ReservedCapacity = GrpcWrappers.Topic.ControlPlane.MeteringMode.ReservedCapacity,
    RequestUnits = GrpcWrappers.Topic.ControlPlane.MeteringMode.RequestUnits
}
