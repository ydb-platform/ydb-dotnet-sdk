using EfCore.Ydb.FunctionalTests.ModelBuilding;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.ModelBuilding;

public class YdbModelBuilderGenericTest : YdbModelBuilderTestBase
{
    public class YdbGenericNonRelationship(YdbModelBuilderFixture fixture) : YdbNonRelationship(fixture)
    {
        [ConditionalFact(Skip = "TODO: Not ready yet")]
        public override void Element_types_can_have_precision_and_scale() => base.Element_types_can_have_precision_and_scale();

        [ConditionalFact(Skip = "TODO: Not ready yet")]
        public override void Element_types_have_default_precision_and_scale() =>
            base.Element_types_have_default_precision_and_scale();

        [ConditionalFact(Skip = "TODO: Not ready yet")]
        public override void Element_types_have_default_unicode() => base.Element_types_have_default_unicode();

        protected override TestModelBuilder CreateModelBuilder(Action<ModelConfigurationBuilder>? configure)
            => new GenericTestModelBuilder(Fixture, configure);
    }

    public class YdbGenericComplexType(YdbModelBuilderFixture fixture) : YdbComplexType(fixture)
    {
        protected override TestModelBuilder CreateModelBuilder(Action<ModelConfigurationBuilder>? configure)
            => new GenericTestModelBuilder(Fixture, configure);
    }

    public class YdbGenericInheritance(YdbModelBuilderFixture fixture) : YdbInheritance(fixture)
    {
        protected override TestModelBuilder CreateModelBuilder(Action<ModelConfigurationBuilder>? configure)
            => new GenericTestModelBuilder(Fixture, configure);
    }

    public class YdbGenericOneToMany(YdbModelBuilderFixture fixture) : YdbOneToMany(fixture)
    {
        protected override TestModelBuilder CreateModelBuilder(Action<ModelConfigurationBuilder>? configure)
            => new GenericTestModelBuilder(Fixture, configure);
    }

    public class YdbGenericManyToOne(YdbModelBuilderFixture fixture) : YdbManyToOne(fixture)
    {
        protected override TestModelBuilder CreateModelBuilder(Action<ModelConfigurationBuilder>? configure)
            => new GenericTestModelBuilder(Fixture, configure);
    }

    public class YdbGenericOneToOne(YdbModelBuilderFixture fixture) : YdbOneToOne(fixture)
    {
        protected override TestModelBuilder CreateModelBuilder(Action<ModelConfigurationBuilder>? configure)
            => new GenericTestModelBuilder(Fixture, configure);
    }

    public class YdbGenericManyToMany(YdbModelBuilderFixture fixture) : YdbManyToMany(fixture)
    {
        protected override TestModelBuilder CreateModelBuilder(Action<ModelConfigurationBuilder>? configure)
            => new GenericTestModelBuilder(Fixture, configure);
    }

    public class YdbGenericOwnedTypes(YdbModelBuilderFixture fixture) : YdbOwnedTypes(fixture)
    {
        [ConditionalFact(Skip = "TODO: Not ready yet")]
        public override void Can_configure_one_to_one_owned_type_with_fields() =>
            base.Can_configure_one_to_one_owned_type_with_fields();

        [ConditionalFact(Skip = "TODO: Not ready yet")]
        public override void Shared_type_entity_types_with_FK_to_another_entity_works() =>
            base.Shared_type_entity_types_with_FK_to_another_entity_works();


        protected override TestModelBuilder CreateModelBuilder(Action<ModelConfigurationBuilder>? configure)
            => new GenericTestModelBuilder(Fixture, configure);
    }
}
