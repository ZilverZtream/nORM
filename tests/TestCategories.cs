namespace nORM.Tests;

/// <summary>
/// Constants for xUnit <c>[Trait("Category", ...)]</c> annotations.
/// Use these to filter test runs in CI:
/// <list type="bullet">
///   <item><description><c>Fast</c> — pure in-process unit tests, no I/O. Always run in the quick gate.</description></item>
///   <item><description><c>LiveProvider</c> — tests that require a real database server (SQL Server, MySQL, PostgreSQL).
///     Excluded from the quick gate; run in live/full/rc gates only.</description></item>
///   <item><description><c>Stress</c> — long-running concurrency or fault-injection loops.
///     Run in full/rc gates. May be skipped in developer quick-runs.</description></item>
///   <item><description><c>PackageConsumer</c> — smoke tests that unzip and inspect the built .nupkg.
///     Require a prior <c>dotnet pack</c>; run after the package step in the release gate.</description></item>
/// </list>
/// Additional RC routing categories are documented in docs/test-suite-ownership.md.
/// </summary>
public static class TestCategory
{
    /// <summary>Pure in-process unit tests with no external I/O.</summary>
    public const string Fast = "Fast";

    /// <summary>Tests that require a configured live database connection (NORM_TEST_* env vars).</summary>
    public const string LiveProvider = "LiveProvider";

    /// <summary>Long-running concurrency, adversarial, or fault-injection stress loops.</summary>
    public const string Stress = "Stress";

    /// <summary>Provider SQL, behavior, migration, and binding parity tests selected by release gates.</summary>
    public const string ProviderParity = "ProviderParity";

    /// <summary>Navigation stress and cancellation tests selected by the RC release gate.</summary>
    public const string NavigationStress = "NavigationStress";

    /// <summary>Transaction stress and lifecycle tests selected by the RC release gate.</summary>
    public const string TransactionStress = "TransactionStress";

    /// <summary>Compiled-query stress and SQL shape tests selected by the RC release gate.</summary>
    public const string CompiledQueryStress = "CompiledQueryStress";

    /// <summary>Provider and source-generator parity tests selected by the RC release gate.</summary>
    public const string ProviderSourceGenParity = "ProviderSourceGenParity";

    /// <summary>Bulk-operation provider parity tests selected by the RC release gate.</summary>
    public const string BulkProviderParity = "BulkProviderParity";

    /// <summary>Migration provider parity, cancellation, and replay tests selected by the RC release gate.</summary>
    public const string MigrationParity = "MigrationParity";

    /// <summary>Cache memory-bound and cache-lock tests selected by the RC release gate.</summary>
    public const string CacheMemory = "CacheMemory";

    /// <summary>Concurrency, adversarial, fault-injection, and stress tests selected by the RC release gate.</summary>
    public const string AdversarialConcurrency = "AdversarialConcurrency";

    /// <summary>Tests that inspect or consume the built .nupkg / .snupkg artifacts.</summary>
    public const string PackageConsumer = "PackageConsumer";
}
