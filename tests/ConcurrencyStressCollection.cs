using Xunit;

namespace nORM.Tests;

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class ConcurrencyStressCollection
{
    public const string Name = "nORM concurrency stress tests";
}
