using System.Data.Common;
using Microsoft.Data.Sqlite;
using nORM.Providers;

namespace nORM.Sample.Store;

/// <summary>
/// The live, swappable database configuration behind the entire app — a singleton.
///
/// The Settings page writes to it (add/replace a connection string, switch the active engine); every
/// request reads the ACTIVE engine from it when its <see cref="StoreContext"/> is built by DI. This is
/// what makes the sample's headline claim real: <b>change the database engine while the app is running
/// and every screen keeps working</b>, because the application code never names a provider — only this
/// object decides which one is live.
///
/// Seeded from environment variables (<c>NORM_SAMPLE_*</c>, then the live-test <c>NORM_TEST_*</c>) so a
/// configured dev box lights up every engine immediately; the UI can override any of them at runtime.
/// </summary>
public sealed class ProviderSettings
{
    private readonly object _gate = new();
    private readonly Dictionary<StoreProviderKind, string?> _connectionStrings = new();
    private StoreProviderKind _active = StoreProviderKind.Sqlite;

    public ProviderSettings()
    {
        _connectionStrings[StoreProviderKind.Sqlite] = SqliteDataSource();
        _connectionStrings[StoreProviderKind.SqlServer] = EnvConnectionString("NORM_SAMPLE_SQLSERVER", "NORM_TEST_SQLSERVER");
        _connectionStrings[StoreProviderKind.Postgres] = EnvConnectionString("NORM_SAMPLE_POSTGRES", "NORM_TEST_POSTGRES");
        _connectionStrings[StoreProviderKind.MySql] = EnvConnectionString("NORM_SAMPLE_MYSQL", "NORM_TEST_MYSQL");
    }

    /// <summary>The engine every new request currently runs against.</summary>
    public StoreProviderKind ActiveKind
    {
        get { lock (_gate) return _active; }
    }

    public StoreProvider ActiveProvider => Describe(ActiveKind);

    public bool IsConfigured(StoreProviderKind kind)
    {
        lock (_gate)
            return kind == StoreProviderKind.Sqlite || !string.IsNullOrWhiteSpace(_connectionStrings[kind]);
    }

    public string? GetConnectionString(StoreProviderKind kind)
    {
        lock (_gate)
            return _connectionStrings.TryGetValue(kind, out var cs) ? cs : null;
    }

    /// <summary>Adds or replaces the connection string the app will use for <paramref name="kind"/>.</summary>
    public void SetConnectionString(StoreProviderKind kind, string connectionString)
    {
        if (kind == StoreProviderKind.Sqlite)
            throw new InvalidOperationException("SQLite uses a bundled local file and needs no connection string.");
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string cannot be empty.", nameof(connectionString));
        lock (_gate)
            _connectionStrings[kind] = connectionString.Trim();
    }

    /// <summary>Switches the live engine. The caller is responsible for bootstrapping schema + seed first.</summary>
    public void Activate(StoreProviderKind kind)
    {
        lock (_gate)
        {
            if (kind != StoreProviderKind.Sqlite && string.IsNullOrWhiteSpace(_connectionStrings[kind]))
                throw new InvalidOperationException($"No connection string configured for {Describe(kind).Name}.");
            _active = kind;
        }
    }

    /// <summary>Opens a fresh, owned connection for a given engine (caller/DI disposes it).</summary>
    public DbConnection OpenConnection(StoreProviderKind kind)
    {
        string? connectionString;
        lock (_gate) connectionString = _connectionStrings[kind];

        if (kind == StoreProviderKind.Sqlite)
        {
            var connection = new SqliteConnection(connectionString);
            connection.Open();
            return connection;
        }

        if (string.IsNullOrWhiteSpace(connectionString))
            throw new InvalidOperationException($"No connection string configured for {Describe(kind).Name}.");

        var conn = CreateDriverConnection(kind, connectionString);
        conn.Open();
        return conn;
    }

    /// <summary>A new nORM provider instance for a given engine.</summary>
    public static DatabaseProvider CreateDatabaseProvider(StoreProviderKind kind) => kind switch
    {
        StoreProviderKind.Sqlite => new SqliteProvider(),
        StoreProviderKind.SqlServer => new SqlServerProvider(),
        StoreProviderKind.Postgres => new PostgresProvider(),
        StoreProviderKind.MySql => new MySqlProvider(),
        _ => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    /// <summary>
    /// Verifies a candidate connection opens and reports the connected server version — the "Test"
    /// button on the Settings page. Never throws; returns a structured result for the UI.
    /// </summary>
    public async Task<ConnectionTestResult> TestConnectionAsync(StoreProviderKind kind, string? connectionString, CancellationToken ct = default)
    {
        connectionString = string.IsNullOrWhiteSpace(connectionString) ? GetConnectionString(kind) : connectionString.Trim();
        if (kind != StoreProviderKind.Sqlite && string.IsNullOrWhiteSpace(connectionString))
            return new ConnectionTestResult(false, null, "No connection string provided.");

        try
        {
            await using var connection = kind == StoreProviderKind.Sqlite
                ? new SqliteConnection(connectionString ?? SqliteDataSource())
                : CreateDriverConnection(kind, connectionString!);
            await connection.OpenAsync(ct);
            var version = connection.ServerVersion;
            return new ConnectionTestResult(true, string.IsNullOrWhiteSpace(version) ? "(connected)" : version, null);
        }
        catch (Exception ex)
        {
            return new ConnectionTestResult(false, null, ex.Message);
        }
    }

    /// <summary>The state the Settings UI renders: every engine, whether it is configured/active, redacted.</summary>
    public IReadOnlyList<ProviderStatus> Snapshot()
    {
        lock (_gate)
        {
            return Enum.GetValues<StoreProviderKind>()
                .Select(kind => new ProviderStatus(
                    Describe(kind).Name,
                    Configured: kind == StoreProviderKind.Sqlite || !string.IsNullOrWhiteSpace(_connectionStrings[kind]),
                    Active: kind == _active,
                    RedactedConnectionString: Redact(kind, _connectionStrings[kind])))
                .ToList();
        }
    }

    public static StoreProvider Describe(StoreProviderKind kind) => kind switch
    {
        StoreProviderKind.Sqlite => new StoreProvider(StoreProviderKind.Sqlite, "sqlite"),
        StoreProviderKind.SqlServer => new StoreProvider(StoreProviderKind.SqlServer, "sqlserver"),
        StoreProviderKind.Postgres => new StoreProvider(StoreProviderKind.Postgres, "postgres"),
        StoreProviderKind.MySql => new StoreProvider(StoreProviderKind.MySql, "mysql"),
        _ => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static DbConnection CreateDriverConnection(StoreProviderKind kind, string connectionString)
    {
        var typeName = kind switch
        {
            StoreProviderKind.SqlServer => "Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient",
            StoreProviderKind.Postgres => "Npgsql.NpgsqlConnection, Npgsql",
            StoreProviderKind.MySql => "MySqlConnector.MySqlConnection, MySqlConnector",
            _ => throw new ArgumentOutOfRangeException(nameof(kind))
        };
        var type = Type.GetType(typeName)
            ?? throw new InvalidOperationException($"Could not load '{typeName}'. Ensure the provider driver package is restored.");
        var connection = (DbConnection)Activator.CreateInstance(type)!;
        connection.ConnectionString = connectionString;
        return connection;
    }

    private static string SqliteDataSource()
    {
        var dir = Path.Combine(AppContext.BaseDirectory, "data");
        Directory.CreateDirectory(dir);
        return $"Data Source={Path.Combine(dir, "nORM.Sample.Store.sqlite.db")}";
    }

    private static string? EnvConnectionString(params string[] names)
    {
        foreach (var name in names)
        {
            var value = Environment.GetEnvironmentVariable(name);
            if (!string.IsNullOrWhiteSpace(value)) return value;
            var alias = Environment.GetEnvironmentVariable(name + "_CS");
            if (!string.IsNullOrWhiteSpace(alias)) return alias;
        }
        return null;
    }

    /// <summary>Hides secrets before a connection string is sent to the browser.</summary>
    private static string? Redact(StoreProviderKind kind, string? connectionString)
    {
        if (kind == StoreProviderKind.Sqlite) return "bundled local file";
        if (string.IsNullOrWhiteSpace(connectionString)) return null;
        var parts = connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var redacted = parts.Select(p =>
        {
            var eq = p.IndexOf('=');
            if (eq <= 0) return p;
            var key = p[..eq].Trim();
            return key.ToLowerInvariant() is "password" or "pwd"
                ? $"{key}=********"
                : p;
        });
        return string.Join("; ", redacted);
    }
}

public sealed record ConnectionTestResult(bool Ok, string? ServerVersion, string? Error);

public sealed record ProviderStatus(string Provider, bool Configured, bool Active, string? RedactedConnectionString);
