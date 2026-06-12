#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldDiagnosticsWriter
    {
        public static string CategoryForCompositeForeignKey()
            => "relationship";

        public static string CategoryForPossibleJoinTable()
            => "many-to-many";

        public static string CategoryForUnsupportedFeature(string kind)
            => kind switch
            {
                "ReferentialAction" or "RelationshipPrincipalKey" or "RelationshipDependentKey" => "relationship",
                "PartialIndex" or "ExpressionIndex" or "IncludedColumnIndex" or "DescendingIndex" or "PrefixIndex" or "ProviderSpecificIndex" => "index",
                "Trigger" or "TemporalTable" => "database-object",
                "MissingPrimaryKey" => "table-shape",
                _ => "schema-feature"
            };

        public static string CategoryForSkippedObject(string kind)
            => kind switch
            {
                "View" or "MaterializedView" => "query-object",
                "Routine" or "Event" => "routine",
                "Sequence" => "key-generation",
                "VirtualTable" or "VirtualTableShadow" => "virtual-table",
                _ => "database-object"
            };
    }
}
