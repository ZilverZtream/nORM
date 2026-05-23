using Xunit;

namespace nORM.Tests;

[CollectionDefinition(Name, DisableParallelization = true)]
[Xunit.Trait("Category", "Stress")]
public sealed class ConcurrencyStressCollection
{
    public const string Name = "nORM concurrency stress tests";
}
