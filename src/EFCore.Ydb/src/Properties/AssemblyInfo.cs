using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore.Design;

[assembly: DesignTimeProviderServices("EntityFrameworkCore.Ydb.Design.Internal.YdbDesignTimeServices")]
[assembly: InternalsVisibleTo("EntityFrameworkCore.Ydb.FunctionalTests")]
