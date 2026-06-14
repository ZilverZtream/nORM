#nullable enable
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    private static async Task SetupProviderSpecificIndexesAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, ProviderIndexTable, provider.Escape(ProviderIndexTable)));

        var table = provider.Escape(ProviderIndexTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var active = provider.Escape("Active");
        var includedValue = provider.Escape("IncludedValue");
        var activeType = kind == ProviderKind.SqlServer ? "BIT" : kind == ProviderKind.Postgres ? "BOOLEAN" : "INTEGER";
        var activePredicate = kind == ProviderKind.SqlServer
            ? $"{active} = 1"
            : kind == ProviderKind.Postgres
                ? $"{active} = TRUE"
                : $"{active} = 1";

        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL, {active} {activeType} NOT NULL, {includedValue} {IntType(kind)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderPartialIndex)} ON {table} ({name}) WHERE {activePredicate}");
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderDescendingIndex)} ON {table} ({name} DESC)");

        if (kind is ProviderKind.Postgres or ProviderKind.Sqlite)
        {
            await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderExpressionIndex)} ON {table} (lower({name}))");
            await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderPartialExpressionIndex)} ON {table} (lower({name})) WHERE {activePredicate}");
            await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderExpressionDescendingIndex)} ON {table} (lower({name}) DESC)");
        }

        if (kind is ProviderKind.Postgres)
            await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderExpressionLiteralDescIndex)} ON {table} (strpos({name}, ' DESC'))");

        if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
            await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderIncludedIndex)} ON {table} ({name}) INCLUDE ({includedValue})");
    }

    private static async Task SetupMySqlPrefixIndexAsync(DbConnection connection, DatabaseProvider provider)
    {
        await ExecuteAsync(connection, DropTable(ProviderKind.MySql, ProviderIndexTable, provider.Escape(ProviderIndexTable)));

        var table = provider.Escape(ProviderIndexTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(ProviderKind.MySql)} NOT NULL PRIMARY KEY, {name} {TextType(ProviderKind.MySql, 80)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderPrefixIndex)} ON {table} ({name}(8))");
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderFullPrefixIndex)} ON {table} ({name}(80))");
    }

    private static async Task SetupMySqlExpressionIndexAsync(DbConnection connection, DatabaseProvider provider)
    {
        await ExecuteAsync(connection, DropTable(ProviderKind.MySql, ProviderIndexTable, provider.Escape(ProviderIndexTable)));

        var table = provider.Escape(ProviderIndexTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var score = provider.Escape("Score");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(ProviderKind.MySql)} NOT NULL PRIMARY KEY, {name} {TextType(ProviderKind.MySql, 80)} NOT NULL, {score} {IntType(ProviderKind.MySql)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderExpressionIndex)} ON {table} ((LOWER({name})), {score})");
    }

    private static async Task SetupProviderSpecificAccessMethodIndexAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, ProviderIndexTable, provider.Escape(ProviderIndexTable)));

        var table = provider.Escape(ProviderIndexTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var score = provider.Escape("Score");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 160)} NOT NULL, {score} {IntType(kind)} NOT NULL)");

        switch (kind)
        {
            case ProviderKind.SqlServer:
                await ExecuteAsync(connection, $"CREATE NONCLUSTERED COLUMNSTORE INDEX {provider.Escape(ProviderSpecificIndex)} ON {table} ({score})");
                break;
            case ProviderKind.Postgres:
                await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderSpecificIndex)} ON {table} USING hash (lower({name}))");
                break;
            case ProviderKind.MySql:
                await ExecuteAsync(connection, $"CREATE FULLTEXT INDEX {provider.Escape(ProviderSpecificIndex)} ON {table} ({name})");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported provider-specific access-method index test provider.");
        }
    }

    private static async Task SetupPostgresExpressionIncludedIndexAsync(DbConnection connection, DatabaseProvider provider)
    {
        await ExecuteAsync(connection, DropTable(ProviderKind.Postgres, ProviderIndexTable, provider.Escape(ProviderIndexTable)));

        var table = provider.Escape(ProviderIndexTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var score = provider.Escape("Score");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(ProviderKind.Postgres)} NOT NULL PRIMARY KEY, {name} {TextType(ProviderKind.Postgres, 160)} NOT NULL, {score} {IntType(ProviderKind.Postgres)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderExpressionIncludedIndex)} ON {table} (lower({name})) INCLUDE ({score})");
    }

    private static async Task SetupPostgresProviderSpecificBtreeOptionIndexAsync(DbConnection connection, DatabaseProvider provider)
    {
        await ExecuteAsync(connection, DropTable(ProviderKind.Postgres, ProviderIndexTable, provider.Escape(ProviderIndexTable)));

        var table = provider.Escape(ProviderIndexTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(ProviderKind.Postgres)} NOT NULL PRIMARY KEY, {name} {TextType(ProviderKind.Postgres, 160)} NULL)");
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderSpecificIndex)} ON {table} ({name} ASC NULLS FIRST)");
    }

    private static async Task SetupPostgresExpressionBtreeOptionIndexAsync(DbConnection connection, DatabaseProvider provider)
    {
        await ExecuteAsync(connection, DropTable(ProviderKind.Postgres, ProviderIndexTable, provider.Escape(ProviderIndexTable)));

        var table = provider.Escape(ProviderIndexTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(ProviderKind.Postgres)} NOT NULL PRIMARY KEY, {name} {TextType(ProviderKind.Postgres, 160)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderSpecificIndex)} ON {table} (lower({name}) text_pattern_ops)");
    }

    private static async Task SetupPostgresNullsNotDistinctUniqueIndexAsync(DbConnection connection, DatabaseProvider provider)
    {
        await ExecuteAsync(connection, DropTable(ProviderKind.Postgres, ProviderIndexTable, provider.Escape(ProviderIndexTable)));

        var table = provider.Escape(ProviderIndexTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(ProviderKind.Postgres)} NOT NULL PRIMARY KEY, {name} {TextType(ProviderKind.Postgres, 160)} NULL)");
        await ExecuteAsync(connection, $"CREATE UNIQUE INDEX {provider.Escape(ProviderSpecificIndex)} ON {table} ({name}) NULLS NOT DISTINCT");
    }
}
