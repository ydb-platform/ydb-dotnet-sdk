using Ef.Ydb.Diagnostics.Internal;
using Ef.Ydb.Infrastructure;
using Ef.Ydb.Infrastructure.Internal;
using Ef.Ydb.Metadata.Conventions;
using Ef.Ydb.Metadata.Internal;
using Ef.Ydb.Migrations;
using Ef.Ydb.Migrations.Internal;
using Ef.Ydb.Query.Internal;
using Ef.Ydb.Storage.Internal;
using Ef.Ydb.Update.Internal;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Microsoft.Extensions.DependencyInjection;

namespace Ef.Ydb.Extensions;

public static class YdbServiceCollectionExtensions
{
    public static IServiceCollection AddEntityFrameworkYdb(this IServiceCollection serviceCollection)
    {
        new EntityFrameworkYdbServicesBuilder(serviceCollection)
            .TryAdd<LoggingDefinitions, YdbLoggingDefinitions>()
            .TryAdd<IDatabaseProvider, DatabaseProvider<YdbOptionsExtension>>()
            .TryAdd<IRelationalTypeMappingSource, YdbTypeMappingSource>()
            .TryAdd<ISqlGenerationHelper, YdbSqlGenerationHelper>()
            .TryAdd<IRelationalAnnotationProvider, YdbAnnotationProvider>()
            .TryAdd<IModelValidator, YdbModelValidator>()
            .TryAdd<IProviderConventionSetBuilder, YdbConventionSetBuilder>()
            .TryAdd<IUpdateSqlGenerator, YdbUpdateSqlGenerator>()
            .TryAdd<IModificationCommandFactory, YdbModificationCommandFactory>()
            .TryAdd<IModificationCommandBatchFactory, YdbModificationCommandBatchFactory>()
            .TryAdd<IRelationalConnection>(p => p.GetRequiredService<IYdbRelationalConnection>())
            .TryAdd<IMigrationsSqlGenerator, YdbMigrationsSqlGenerator>()
            .TryAdd<IRelationalDatabaseCreator, YdbDatabaseCreator>()
            .TryAdd<IHistoryRepository, YdbHistoryRepository>()
            .TryAdd<IQueryableMethodTranslatingExpressionVisitorFactory,
                YdbQueryableMethodTranslatingExpressionVisitorFactory>()
            .TryAdd<IMethodCallTranslatorProvider, YdbMethodCallTranslatorProvider>()
            .TryAdd<IAggregateMethodCallTranslatorProvider, YdbAggregateMethodCallTranslatorProvider>()
            .TryAdd<IMemberTranslatorProvider, YdbMemberTranslatorProvider>()
            .TryAdd<IQuerySqlGeneratorFactory, YdbQuerySqlGeneratorFactory>()
            .TryAdd<IRelationalSqlTranslatingExpressionVisitorFactory, YdbSqlTranslatingExpressionVisitorFactory>()
            .TryAdd<IQueryTranslationPostprocessorFactory, YdbQueryTranslationPostprocessorFactory>()
            .TryAdd<IRelationalParameterBasedSqlProcessorFactory, YdbParameterBasedSqlProcessorFactory>()
            .TryAdd<ISqlExpressionFactory, YdbSqlExpressionFactory>()
            .TryAdd<IQueryCompilationContextFactory, YdbQueryCompilationContextFactory>()
            .TryAddProviderSpecificServices(
                b => b
                    .TryAddScoped<IYdbRelationalConnection, YdbRelationalConnection>()
                    .TryAddScoped<IDbCommandInterceptor, YdbCommandInterceptor>())
            .TryAddCoreServices();

        return serviceCollection;
    }
}
