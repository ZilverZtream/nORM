using Xunit;

namespace nORM.Tests;

/// <summary>
/// xUnit collection that serializes <see cref="PackageConsumerIntegrationTests"/> so the
/// shared <c>src/bin/Release/*.nupkg</c> outputs cannot be raced by parallel test executions.
/// </summary>
[CollectionDefinition("PackageConsumerSerial", DisableParallelization = true)]
public class PackageConsumerCollection
{
}
