using System.Reflection;
using EfCore.Ydb.Abstractions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;

namespace EfCore.Ydb.Metadata.Conventions;

public class YdbStringAttributeConvention
    : PropertyAttributeConventionBase<YdbStringAttribute>
{
    public YdbStringAttributeConvention(ProviderConventionSetBuilderDependencies dependencies) : base(dependencies)
    {
    }

    protected override void ProcessPropertyAdded(
        IConventionPropertyBuilder propertyBuilder,
        YdbStringAttribute attribute,
        MemberInfo clrMember,
        IConventionContext context
    )
    {
        propertyBuilder.HasColumnType("string");
    }
}
