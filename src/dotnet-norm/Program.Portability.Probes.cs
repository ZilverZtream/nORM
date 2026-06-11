using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using nORM.Cli;
using nORM.Configuration;
using nORM.Providers;
using nORM.Security;

partial class Program
{
    static IReadOnlyList<string>? ParseProviderTargetList(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        return value
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(static provider => NormalizeProviderTargetName(provider))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    static string NormalizeProviderTargetName(string providerName)
        => NormalizeProviderName(providerName);

    static void ProbeProviderTargetConnection(
        string providerName,
        string connectionString,
        IDictionary<string, Version?> targetVersions,
        ICollection<ProviderMobilityFinding> findings)
    {
        try
        {
            var validated = ConnectionStringValidator.Validate(connectionString, providerName);
            using var connection = CreateConnection(providerName, validated.ConnectionString);
            connection.Open();
            var provider = CreateProvider(providerName);
            provider.InitializeConnection(connection);
            ProbeProviderTargetFeatures(providerName, connection, findings);
            targetVersions[providerName] = ProviderMobilityTranslator.ParseProviderVersion(connection.ServerVersion);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException and not StackOverflowException)
        {
            findings.Add(new ProviderMobilityFinding(
                "provider-target",
                0,
                "provider-target-open",
                "Error",
                $"Provider target '{providerName}' could not be opened and validated: {RedactConnectionStrings(ex.Message)}",
                "Fix the target connection string, driver, credentials, network access, or server version before claiming live provider mobility evidence."));
        }
    }

    static void ProbeProviderTargetFeatures(
        string providerName,
        DbConnection connection,
        ICollection<ProviderMobilityFinding> findings)
    {
        var scalarProbes = providerName.ToLowerInvariant() switch
        {
            "sqlite" => new[]
            {
                ("JSON translation", "SELECT json_extract('{\"a\":1}', '$.a')"),
                ("window function", "SELECT ROW_NUMBER() OVER (ORDER BY 1)")
            },
            "sqlserver" => new[]
            {
                ("JSON translation", "SELECT JSON_VALUE(N'{\"a\":1}', '$.a')"),
                ("window function", "SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 1))")
            },
            "postgres" => new[]
            {
                ("JSON translation", "SELECT ('{\"a\":1}'::jsonb ->> 'a')"),
                ("window function", "SELECT ROW_NUMBER() OVER (ORDER BY 1)")
            },
            "mysql" => new[]
            {
                ("JSON translation", "SELECT JSON_EXTRACT('{\"a\":1}', '$.a')"),
                ("window function", "SELECT ROW_NUMBER() OVER (ORDER BY v) FROM (SELECT 1 AS v) AS t")
            },
            _ => Array.Empty<(string Feature, string Sql)>()
        };

        foreach (var (feature, sql) in scalarProbes)
        {
            ProbeProviderTargetFeature(providerName, feature, findings, () => ExecuteProviderTargetScalar(connection, sql));
        }

        ProbeProviderTargetFeature(providerName, "savepoint", findings, () => ProbeProviderTargetSavepoint(providerName, connection));
        ProbeProviderTargetFeature(providerName, "generated-value retrieval", findings, () => ProbeProviderTargetGeneratedValue(providerName, connection));
        ProbeProviderTargetFeature(providerName, "rename column DDL", findings, () => ProbeProviderTargetRenameColumn(providerName, connection));
        ProbeProviderTargetFeature(providerName, "idempotent insert/ignore", findings, () => ProbeProviderTargetIdempotentInsert(providerName, connection));
    }

    static void ProbeProviderTargetFeature(
        string providerName,
        string feature,
        ICollection<ProviderMobilityFinding> findings,
        Action probe)
    {
        try
        {
            probe();
        }
        catch (Exception ex) when (ex is not OutOfMemoryException and not StackOverflowException)
        {
            findings.Add(new ProviderMobilityFinding(
                "provider-target",
                0,
                "provider-target-capability",
                "Error",
                $"Provider target '{providerName}' failed the {feature} probe: {RedactConnectionStrings(ex.Message)}",
                "Enable the required database feature/extension or remove that provider target before claiming live provider mobility evidence."));
        }
    }

    static object? ExecuteProviderTargetScalar(DbConnection connection, string sql, DbTransaction? transaction = null)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;
        return command.ExecuteScalar();
    }

    static int ExecuteProviderTargetNonQuery(DbConnection connection, string sql, DbTransaction? transaction = null)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;
        return command.ExecuteNonQuery();
    }

    static string ProviderTargetProbeName()
        => "__norm_cert_" + Guid.NewGuid().ToString("N")[..16];

    static string ProviderTargetTable(DatabaseProvider provider, string tableName, string? schema = null)
        => string.IsNullOrWhiteSpace(schema)
            ? provider.Escape(tableName)
            : provider.Escape(schema) + "." + provider.Escape(tableName);

    static void ProbeProviderTargetSavepoint(string providerName, DbConnection connection)
    {
        using var transaction = connection.BeginTransaction();
        try
        {
            if (providerName.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
            {
                ExecuteProviderTargetNonQuery(connection, "SAVE TRANSACTION norm_cert_sp", transaction);
                ExecuteProviderTargetNonQuery(connection, "ROLLBACK TRANSACTION norm_cert_sp", transaction);
            }
            else
            {
                ExecuteProviderTargetNonQuery(connection, "SAVEPOINT norm_cert_sp", transaction);
                ExecuteProviderTargetNonQuery(connection, "ROLLBACK TO SAVEPOINT norm_cert_sp", transaction);
                ExecuteProviderTargetNonQuery(connection, "RELEASE SAVEPOINT norm_cert_sp", transaction);
            }

            transaction.Rollback();
        }
        catch
        {
            try { transaction.Rollback(); } catch { }
            throw;
        }
    }

    static void ProbeProviderTargetGeneratedValue(string providerName, DbConnection connection)
    {
        var provider = CreateProvider(providerName);
        var tableName = ProviderTargetProbeName();
        var table = ProviderTargetTable(provider, tableName, providerName.Equals("sqlserver", StringComparison.OrdinalIgnoreCase) ? "dbo" : null);

        try
        {
            switch (providerName.ToLowerInvariant())
            {
                case "sqlite":
                    ExecuteProviderTargetNonQuery(connection, $"CREATE TEMP TABLE {table} ({provider.Escape("Id")} INTEGER PRIMARY KEY AUTOINCREMENT, {provider.Escape("Name")} TEXT NULL)");
                    ExecuteProviderTargetNonQuery(connection, $"INSERT INTO {table} ({provider.Escape("Name")}) VALUES ('a')");
                    EnsureProviderTargetScalarValue("generated-value retrieval", ExecuteProviderTargetScalar(connection, "SELECT last_insert_rowid()"));
                    break;
                case "sqlserver":
                    ExecuteProviderTargetNonQuery(connection, $"CREATE TABLE {table} ({provider.Escape("Id")} int IDENTITY(1,1) NOT NULL PRIMARY KEY, {provider.Escape("Name")} nvarchar(20) NULL)");
                    EnsureProviderTargetScalarValue("generated-value retrieval", ExecuteProviderTargetScalar(connection, $"INSERT INTO {table} ({provider.Escape("Name")}) OUTPUT INSERTED.{provider.Escape("Id")} VALUES (N'a')"));
                    break;
                case "postgres":
                    ExecuteProviderTargetNonQuery(connection, $"CREATE TEMP TABLE {table} ({provider.Escape("Id")} integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, {provider.Escape("Name")} text NULL)");
                    EnsureProviderTargetScalarValue("generated-value retrieval", ExecuteProviderTargetScalar(connection, $"INSERT INTO {table} ({provider.Escape("Name")}) VALUES ('a') RETURNING {provider.Escape("Id")}"));
                    break;
                case "mysql":
                    ExecuteProviderTargetNonQuery(connection, $"CREATE TEMPORARY TABLE {table} ({provider.Escape("Id")} INT NOT NULL AUTO_INCREMENT PRIMARY KEY, {provider.Escape("Name")} varchar(20) NULL)");
                    ExecuteProviderTargetNonQuery(connection, $"INSERT INTO {table} ({provider.Escape("Name")}) VALUES ('a')");
                    EnsureProviderTargetScalarValue("generated-value retrieval", ExecuteProviderTargetScalar(connection, "SELECT LAST_INSERT_ID()"));
                    break;
            }
        }
        finally
        {
            DropProviderTargetProbeTable(providerName, connection, provider, tableName);
        }
    }

    static void ProbeProviderTargetRenameColumn(string providerName, DbConnection connection)
    {
        var provider = CreateProvider(providerName);
        var tableName = ProviderTargetProbeName();
        var table = ProviderTargetTable(provider, tableName, providerName.Equals("sqlserver", StringComparison.OrdinalIgnoreCase) ? "dbo" : null);

        try
        {
            switch (providerName.ToLowerInvariant())
            {
                case "sqlite":
                    ExecuteProviderTargetNonQuery(connection, $"CREATE TEMP TABLE {table} ({provider.Escape("OldName")} INTEGER NOT NULL)");
                    ExecuteProviderTargetNonQuery(connection, $"ALTER TABLE {table} RENAME COLUMN {provider.Escape("OldName")} TO {provider.Escape("NewName")}");
                    break;
                case "sqlserver":
                    ExecuteProviderTargetNonQuery(connection, $"CREATE TABLE {table} ({provider.Escape("OldName")} int NOT NULL)");
                    ExecuteProviderTargetNonQuery(connection, $"EXEC sp_rename N'dbo.{tableName}.OldName', N'NewName', N'COLUMN'");
                    break;
                case "postgres":
                    ExecuteProviderTargetNonQuery(connection, $"CREATE TEMP TABLE {table} ({provider.Escape("OldName")} integer NOT NULL)");
                    ExecuteProviderTargetNonQuery(connection, $"ALTER TABLE {table} RENAME COLUMN {provider.Escape("OldName")} TO {provider.Escape("NewName")}");
                    break;
                case "mysql":
                    ExecuteProviderTargetNonQuery(connection, $"CREATE TEMPORARY TABLE {table} ({provider.Escape("OldName")} INT NOT NULL)");
                    ExecuteProviderTargetNonQuery(connection, $"ALTER TABLE {table} RENAME COLUMN {provider.Escape("OldName")} TO {provider.Escape("NewName")}");
                    break;
            }
        }
        finally
        {
            DropProviderTargetProbeTable(providerName, connection, provider, tableName);
        }
    }

    static void ProbeProviderTargetIdempotentInsert(string providerName, DbConnection connection)
    {
        var provider = CreateProvider(providerName);
        var tableName = ProviderTargetProbeName();
        var table = ProviderTargetTable(provider, tableName, providerName.Equals("sqlserver", StringComparison.OrdinalIgnoreCase) ? "dbo" : null);
        var code = provider.Escape("Code");

        try
        {
            switch (providerName.ToLowerInvariant())
            {
                case "sqlite":
                    ExecuteProviderTargetNonQuery(connection, $"CREATE TEMP TABLE {table} ({code} INTEGER NOT NULL UNIQUE)");
                    ExecuteProviderTargetNonQuery(connection, $"INSERT OR IGNORE INTO {table} ({code}) VALUES (1)");
                    ExecuteProviderTargetNonQuery(connection, $"INSERT OR IGNORE INTO {table} ({code}) VALUES (1)");
                    break;
                case "sqlserver":
                    ExecuteProviderTargetNonQuery(connection, $"CREATE TABLE {table} ({code} int NOT NULL UNIQUE)");
                    ExecuteProviderTargetNonQuery(connection, $"IF NOT EXISTS (SELECT 1 FROM {table} WHERE {code} = 1) INSERT INTO {table} ({code}) VALUES (1)");
                    ExecuteProviderTargetNonQuery(connection, $"IF NOT EXISTS (SELECT 1 FROM {table} WHERE {code} = 1) INSERT INTO {table} ({code}) VALUES (1)");
                    break;
                case "postgres":
                    ExecuteProviderTargetNonQuery(connection, $"CREATE TEMP TABLE {table} ({code} integer NOT NULL UNIQUE)");
                    ExecuteProviderTargetNonQuery(connection, $"INSERT INTO {table} ({code}) VALUES (1) ON CONFLICT DO NOTHING");
                    ExecuteProviderTargetNonQuery(connection, $"INSERT INTO {table} ({code}) VALUES (1) ON CONFLICT DO NOTHING");
                    break;
                case "mysql":
                    ExecuteProviderTargetNonQuery(connection, $"CREATE TEMPORARY TABLE {table} ({code} INT NOT NULL UNIQUE)");
                    ExecuteProviderTargetNonQuery(connection, $"INSERT IGNORE INTO {table} ({code}) VALUES (1)");
                    ExecuteProviderTargetNonQuery(connection, $"INSERT IGNORE INTO {table} ({code}) VALUES (1)");
                    break;
            }

            var count = Convert.ToInt64(ExecuteProviderTargetScalar(connection, $"SELECT COUNT(*) FROM {table}") ?? 0);
            if (count != 1)
                throw new InvalidOperationException($"Idempotent insert probe expected one row but found {count}.");
        }
        finally
        {
            DropProviderTargetProbeTable(providerName, connection, provider, tableName);
        }
    }

    static void EnsureProviderTargetScalarValue(string feature, object? value)
    {
        if (value is null || value is DBNull)
            throw new InvalidOperationException($"The {feature} probe returned no value.");
    }

    static void DropProviderTargetProbeTable(string providerName, DbConnection connection, DatabaseProvider provider, string tableName)
    {
        try
        {
            if (providerName.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
            {
                ExecuteProviderTargetNonQuery(
                    connection,
                    $"IF OBJECT_ID(N'dbo.{tableName}', N'U') IS NOT NULL DROP TABLE {ProviderTargetTable(provider, tableName, "dbo")}");
                return;
            }

            var table = ProviderTargetTable(provider, tableName);
            if (providerName.Equals("mysql", StringComparison.OrdinalIgnoreCase))
                ExecuteProviderTargetNonQuery(connection, $"DROP TEMPORARY TABLE IF EXISTS {table}");
            else
                ExecuteProviderTargetNonQuery(connection, $"DROP TABLE IF EXISTS {table}");
        }
        catch
        {
            // Best-effort cleanup; the probe failure path reports the original feature error.
        }
    }
}
