using System;

namespace nORM.Scaffolding
{
    public partial class DynamicEntityTypeGenerator
    {
        internal sealed record ColumnInfo(string ColumnName, string PropertyName, Type PropertyType, bool AllowsNull, bool IsKey, int KeyOrdinal, int SourceOrdinal, bool IsAuto, bool IsComputed, ScaffoldComputedColumn? ComputedColumn, bool IsRowVersion, int? MaxLength, bool? IsUnicode, bool IsFixedLength, ScaffoldDecimalPrecision? DecimalPrecision);

        internal readonly record struct ScaffoldComputedColumn(string Sql, bool Stored);
        internal readonly record struct ScaffoldDecimalPrecision(int Precision, int? Scale);
        internal readonly record struct ScaffoldColumnFacet(int? MaxLength, bool? IsUnicode, bool IsFixedLength);
    }
}
