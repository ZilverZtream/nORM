#nullable enable
using System.Collections.Generic;
using System.Data.Common;

namespace nORM.Scaffolding
{
    public static partial class DatabaseScaffolder
    {
        private static bool ReaderHasColumn(DbDataReader reader, string name)
            => ScaffoldDataReaderHelper.HasColumn(reader, name);

        private static IReadOnlyDictionary<string, object?> BuildSkippedObjectMetadata(ScaffoldSkippedObject obj)
            => ScaffoldSchemaDiscoveryAdapter.BuildSkippedObjectMetadata(obj);

        private static IReadOnlyDictionary<string, object?> BuildUnsupportedFeatureMetadata(ScaffoldUnsupportedFeature feature)
            => ScaffoldDiagnosticsAdapter.BuildUnsupportedFeatureMetadata(feature);

        private static bool TryParseMetadataBoolean(string value, out bool parsed)
            => ScaffoldDiagnosticsAdapter.TryParseMetadataBoolean(value, out parsed);
    }
}
