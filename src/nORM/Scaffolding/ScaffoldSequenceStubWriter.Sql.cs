#nullable enable
using static nORM.Scaffolding.ScaffoldCodeText;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSequenceStubWriter
    {
        private static string FormatSequenceSqlExpression(ScaffoldContextSequenceInfo sequence, string provider)
        {
            var escapedValueAlias = " + Provider.Escape(\"Value\")";
            var sequenceName = FormatProviderEscapedSequenceName(sequence);
            if (provider == "sqlserver")
                return "\"SELECT NEXT VALUE FOR \" + " + sequenceName + " + \" AS \"" + escapedValueAlias;

            return "\"SELECT nextval('\" + (" + sequenceName + ").Replace(\"'\", \"''\") + \"'::regclass) AS \"" + escapedValueAlias;
        }

        private static string FormatProviderEscapedSequenceName(ScaffoldContextSequenceInfo sequence)
        {
            var name = EscapeStringLiteral(sequence.Name);
            if (string.IsNullOrWhiteSpace(sequence.Schema))
                return $"Provider.Escape(\"{name}\")";

            var schema = EscapeStringLiteral(sequence.Schema!);
            return $"Provider.Escape(\"{schema}\") + \".\" + Provider.Escape(\"{name}\")";
        }

        private static string QualifiedSequenceName(ScaffoldContextSequenceInfo sequence)
            => string.IsNullOrWhiteSpace(sequence.Schema) ? sequence.Name : sequence.Schema + "." + sequence.Name;
    }
}
