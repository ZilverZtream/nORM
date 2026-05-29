using Xunit;

namespace nORM.Tests;

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class LiveTemporalProviderCollection
{
    public const string Name = "LiveProviderTemporalSerial";
}
