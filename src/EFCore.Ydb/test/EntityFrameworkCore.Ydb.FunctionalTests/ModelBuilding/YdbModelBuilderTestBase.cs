using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.ModelBuilding;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.ModelBuilding;

public class YdbModelBuilderTestBase : RelationalModelBuilderTest
{
    public abstract class YdbNonRelationship(YdbModelBuilderFixture fixture)
        : RelationalNonRelationshipTestBase(fixture), IClassFixture<YdbModelBuilderFixture>;

    public abstract class YdbComplexType(YdbModelBuilderFixture fixture)
        : RelationalComplexTypeTestBase(fixture), IClassFixture<YdbModelBuilderFixture>;

    public abstract class YdbInheritance(YdbModelBuilderFixture fixture)
        : RelationalInheritanceTestBase(fixture), IClassFixture<YdbModelBuilderFixture>;

    public abstract class YdbOneToMany(YdbModelBuilderFixture fixture)
        : RelationalOneToManyTestBase(fixture), IClassFixture<YdbModelBuilderFixture>;

    public abstract class YdbManyToOne(YdbModelBuilderFixture fixture)
        : RelationalManyToOneTestBase(fixture), IClassFixture<YdbModelBuilderFixture>;

    public abstract class YdbOneToOne(YdbModelBuilderFixture fixture)
        : RelationalOneToOneTestBase(fixture), IClassFixture<YdbModelBuilderFixture>;

    public abstract class YdbManyToMany(YdbModelBuilderFixture fixture)
        : RelationalManyToManyTestBase(fixture), IClassFixture<YdbModelBuilderFixture>;

    public abstract class YdbOwnedTypes(YdbModelBuilderFixture fixture)
        : RelationalOwnedTypesTestBase(fixture), IClassFixture<YdbModelBuilderFixture>;

    public class YdbModelBuilderFixture : RelationalModelBuilderFixture
    {
        public override TestHelpers TestHelpers
            => YdbTestHelpers.Instance;
    }
}
