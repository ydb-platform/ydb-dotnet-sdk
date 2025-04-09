using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.Ydb.Storage.Internal;

public interface IYdbRelationalConnection : IRelationalConnection
{
    IYdbRelationalConnection Clone();
}
