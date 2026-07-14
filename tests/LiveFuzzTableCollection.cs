using Xunit;

namespace nORM.Tests;

// The live fuzz-parity classes (LinqParityFuzzLiveTests and
// LiveProviderSubqueryFuzzParityTests) reuse the same shared entity model, so they
// create and drop the SAME physical tables (FuzzRow_Test/FuzzChild_Test/…) on the
// SAME live servers. Run in parallel they race on the DDL — a duplicate-key
// collision on the catalog (e.g. Postgres pg_type_typname_nsp_index). Serializing
// them in one non-parallel collection reflects that they share that live-DB resource.
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class LiveFuzzTableCollection
{
    public const string Name = "LiveFuzzTableSerial";
}
