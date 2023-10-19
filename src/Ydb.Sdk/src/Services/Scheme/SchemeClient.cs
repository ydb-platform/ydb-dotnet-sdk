using Ydb.Scheme;
using Ydb.Scheme.V1;
using Ydb.Sdk.Client;

namespace Ydb.Sdk.Services.Scheme;

public class ListDirectorySettings : OperationRequestSettings
{
}

public enum SchemeEntryType : uint
{
    Unspecified = 0,
    Directory = 1,
    Table = 2,
    PersQueueGroup = 3,
    Database = 4,
    RtmrVolume = 5,
    BlockStoreVolume = 6,
    CoordinationNode = 7
}

public class Permissions
{
    public Permissions(string subject, IReadOnlyList<string> permissionNames)
    {
        Subject = subject;
        PermissionNames = permissionNames;
    }

    public string Subject { get; }
    public IReadOnlyList<string> PermissionNames { get; }

    internal static Permissions FromProto(Ydb.Scheme.Permissions permissionsProto)
    {
        return new Permissions(
            subject: permissionsProto.Subject,
            permissionNames: permissionsProto.PermissionNames);
    }
}

public class SchemeEntry
{
    internal SchemeEntry(
        string name,
        string owner,
        SchemeEntryType type,
        IReadOnlyList<Permissions> effectivePermissions,
        IReadOnlyList<Permissions> permissions)
    {
        Name = name;
        Owner = owner;
        Type = type;
        EffectivePermissions = effectivePermissions;
        Permissions = permissions;
    }

    public string Name { get; }
    public string Owner { get; }
    public SchemeEntryType Type { get; }
    public IReadOnlyList<Permissions> EffectivePermissions { get; }
    public IReadOnlyList<Permissions> Permissions { get; }

    internal static SchemeEntry FromProto(Entry entryProto)
    {
        var type = Enum.IsDefined(typeof(SchemeEntryType), (uint)entryProto.Type)
            ? (SchemeEntryType)entryProto.Type
            : SchemeEntryType.Unspecified;

        var effectivePermissions = entryProto.EffectivePermissions
            .Select(p => Scheme.Permissions.FromProto(p))
            .ToList();

        var permissions = entryProto.Permissions
            .Select(p => Scheme.Permissions.FromProto(p))
            .ToList();

        return new SchemeEntry(
            name: entryProto.Name,
            owner: entryProto.Owner,
            type: type,
            effectivePermissions: effectivePermissions,
            permissions: permissions);
    }
}

public class ListDirectoryResponse : ResponseWithResultBase<ListDirectoryResponse.ResultData>
{
    internal ListDirectoryResponse(Status status, ResultData? result = null)
        : base(status, result)
    {
    }

    public class ResultData
    {
        internal ResultData(SchemeEntry self, IReadOnlyList<SchemeEntry> children)
        {
            Self = self;
            Children = children;
        }

        public SchemeEntry Self { get; }
        public IReadOnlyList<SchemeEntry> Children { get; }

        internal static ResultData FromProto(ListDirectoryResult resultProto)
        {
            var self = SchemeEntry.FromProto(resultProto.Self);
            var children = resultProto.Children
                .Select(c => SchemeEntry.FromProto(c))
                .ToList();

            return new ResultData(
                self: self,
                children: children
            );
        }
    }
}

public class SchemeClient : ClientBase
{
    public SchemeClient(Driver driver)
        : base(driver)
    {
    }

    public async Task<ListDirectoryResponse> ListDirectory(string path, ListDirectorySettings? settings = null)
    {
        settings ??= new ListDirectorySettings();

        var request = new ListDirectoryRequest
        {
            OperationParams = MakeOperationParams(settings),
            Path = path
        };

        try
        {
            var response = await Driver.UnaryCall(
                method: SchemeService.ListDirectoryMethod,
                request: request,
                settings: settings);

            ListDirectoryResult? resultProto;
            var status = UnpackOperation(response.Data.Operation, out resultProto);

            ListDirectoryResponse.ResultData? result = null;
            if (status.IsSuccess && resultProto != null)
            {
                result = ListDirectoryResponse.ResultData.FromProto(resultProto);
            }

            return new ListDirectoryResponse(status, result);
        }
        catch (Driver.TransportException e)
        {
            return new ListDirectoryResponse(e.Status);
        }
    }
}
