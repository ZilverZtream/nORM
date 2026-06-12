#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldDiagnosticsWriter
    {
        public static string Severity()
            => "Warning";

        public static string CodeForCompositeForeignKey()
            => "SCF001";

        public static string CodeForPossibleJoinTable()
            => "SCF002";

        public static string CodeForUnsupportedFeature(string kind)
            => kind switch
            {
                "Default" => "SCF100",
                "Computed" => "SCF101",
                "CheckConstraint" => "SCF102",
                "Collation" => "SCF103",
                "ProviderSpecificColumnType" => "SCF104",
                "PrecisionScale" => "SCF105",
                "ReferentialAction" => "SCF106",
                "RelationshipPrincipalKey" => "SCF107",
                "RelationshipDependentKey" => "SCF118",
                "RowVersion" => "SCF108",
                "IdentityStrategy" => "SCF109",
                "Trigger" => "SCF110",
                "PartialIndex" => "SCF111",
                "ExpressionIndex" => "SCF112",
                "IncludedColumnIndex" => "SCF113",
                "DescendingIndex" => "SCF114",
                "TemporalTable" => "SCF115",
                "MissingPrimaryKey" => "SCF116",
                "PrefixIndex" => "SCF117",
                "ProviderSpecificIndex" => "SCF119",
                _ => "SCF199"
            };

        public static string CodeForSkippedObject(string kind)
            => kind switch
            {
                "View" => "SCF200",
                "Routine" => "SCF201",
                "Sequence" => "SCF202",
                "Synonym" => "SCF203",
                "MaterializedView" => "SCF204",
                "Event" => "SCF205",
                "VirtualTable" => "SCF206",
                "VirtualTableShadow" => "SCF207",
                _ => "SCF299"
            };
    }
}
