using System.Reflection;
using EntityFrameworkCore.Ydb.Abstractions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;

namespace EntityFrameworkCore.Ydb.Metadata.Conventions;

public class YdbStringAttributeConvention(ProviderConventionSetBuilderDependencies dependencies)
    : PropertyAttributeConventionBase<YdbStringAttribute>(dependencies)
{
    protected override void ProcessPropertyAdded(
        IConventionPropertyBuilder propertyBuilder,
        YdbStringAttribute attribute,
        MemberInfo clrMember,
        IConventionContext context
    ) => propertyBuilder.HasColumnType("string");
}
