using System;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Migration;
using nORM.Providers;

partial class Program
{
    static string[] BuildDesignTimeArgs(string? environment)
        => string.IsNullOrWhiteSpace(environment)
            ? Array.Empty<string>()
            : new[] { "--environment", environment };

    static SchemaSnapshot BuildMigrationSnapshot(Assembly assembly, bool attributeOnly, string[] designTimeArgs)
    {
        var factory = FindDesignTimeFactory(assembly);
        if (factory != null)
        {
            using var ctx = CreateDesignTimeContext(factory.Value.FactoryType, factory.Value.InterfaceType, designTimeArgs);
            Console.WriteLine($"Using design-time DbContext factory {factory.Value.FactoryType.FullName}.");
            return SchemaSnapshotBuilder.Build(ctx);
        }

        if (attributeOnly)
        {
            Console.WriteLine("Using attribute-only model snapshot.");
            return SchemaSnapshotBuilder.Build(assembly);
        }

        var ctxType = assembly.GetTypes()
            .FirstOrDefault(t => t.IsClass && !t.IsAbstract && typeof(DbContext).IsAssignableFrom(t));
        if (ctxType == null)
        {
            throw new InvalidOperationException(
                "No DbContext type was found. Add an INormDesignTimeDbContextFactory<TContext> implementation " +
                "or re-run with --attribute-only to generate from attributes only.");
        }

        try
        {
            using var modelCn = new SqliteConnection("Data Source=:memory:");
            modelCn.Open();
            var provider = new SqliteProvider();
            using var modelCtx = CreateModelContext(ctxType, modelCn, provider);
            Console.WriteLine($"Using fluent model from {ctxType.Name}.");
            return SchemaSnapshotBuilder.Build(modelCtx);
        }
        catch (Exception ex) when (ex is MissingMethodException or TargetInvocationException or InvalidOperationException or MemberAccessException)
        {
            throw new InvalidOperationException(
                $"Could not instantiate DbContext type '{ctxType.FullName}' for migration generation. " +
                "Add an INormDesignTimeDbContextFactory<TContext> implementation or re-run with --attribute-only " +
                "if you intentionally want to ignore fluent model configuration.",
                ex);
        }
    }

    static DbContext CreateModelContext(Type ctxType, DbConnection connection, DatabaseProvider provider)
    {
        var twoArgConstructor = ctxType.GetConstructor(new[] { typeof(DbConnection), typeof(DatabaseProvider) });
        if (twoArgConstructor != null)
            return (DbContext)twoArgConstructor.Invoke(new object[] { connection, provider });

        var threeArgConstructor = ctxType.GetConstructor(new[] { typeof(DbConnection), typeof(DatabaseProvider), typeof(DbContextOptions) });
        if (threeArgConstructor != null)
            return (DbContext)threeArgConstructor.Invoke(new object?[] { connection, provider, null });

        throw new MissingMethodException(ctxType.FullName, ".ctor(DbConnection, DatabaseProvider[, DbContextOptions])");
    }

    static (Type FactoryType, Type InterfaceType)? FindDesignTimeFactory(Assembly assembly)
    {
        foreach (var type in assembly.GetTypes().Where(static t => t.IsClass && !t.IsAbstract))
        {
            var interfaceType = type.GetInterfaces().FirstOrDefault(static i =>
                i.IsGenericType && i.GetGenericTypeDefinition() == typeof(INormDesignTimeDbContextFactory<>));
            if (interfaceType != null)
                return (type, interfaceType);
        }

        return null;
    }

    static DbContext CreateDesignTimeContext(Type factoryType, Type interfaceType, string[] designTimeArgs)
    {
        object factory;
        try
        {
            factory = Activator.CreateInstance(factoryType)
                ?? throw new InvalidOperationException($"Could not create design-time factory '{factoryType.FullName}'.");
        }
        catch (TargetInvocationException ex) when (ex.InnerException != null)
        {
            throw new InvalidOperationException(
                $"Design-time factory '{factoryType.FullName}' constructor failed: {ex.InnerException.Message}",
                ex.InnerException);
        }

        var method = interfaceType.GetMethod(nameof(INormDesignTimeDbContextFactory<DbContext>.CreateDbContext))
            ?? throw new InvalidOperationException($"Design-time factory '{factoryType.FullName}' does not expose CreateDbContext.");
        object? context;
        try
        {
            context = method.Invoke(factory, new object[] { designTimeArgs });
        }
        catch (TargetInvocationException ex) when (ex.InnerException != null)
        {
            throw new InvalidOperationException(
                $"Design-time factory '{factoryType.FullName}' failed while creating the DbContext: {ex.InnerException.Message}",
                ex.InnerException);
        }

        if (context is null)
            throw new InvalidOperationException($"Design-time factory '{factoryType.FullName}' returned null.");

        if (context is not DbContext dbContext)
            throw new InvalidOperationException($"Design-time factory '{factoryType.FullName}' did not return a nORM DbContext.");
        return dbContext;
    }
}
