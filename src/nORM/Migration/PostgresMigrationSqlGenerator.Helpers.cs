#nullable enable
using System;

namespace nORM.Migration
{
    public partial class PostgresMigrationSqlGenerator
    {
        // A type-appropriate zero/empty literal used to backfill existing rows when a dropped NOT NULL column
        // (with no default) is restored on the Down path — PostgreSQL rejects ADD COLUMN ... NOT NULL on a
        // populated table without a default. The literal is applied as a temporary DEFAULT and then dropped so
        // the restored schema keeps no default.
        private static string GetPostgresRestoreFillLiteral(ColumnSchema column)
        {
            var baseType = GetSqlType(column).ToUpperInvariant().Split('(')[0].Trim();
            return baseType switch
            {
                "TEXT" or "VARCHAR" or "CHAR" or "CHARACTER" or "CHARACTER VARYING" => "''",
                "BOOLEAN" or "BOOL" => "FALSE",
                "BYTEA" => "''::bytea",
                "UUID" => "'00000000-0000-0000-0000-000000000000'::uuid",
                "TIMESTAMP" or "TIMESTAMPTZ" => "'1970-01-01 00:00:00'",
                "DATE" => "'1970-01-01'",
                "TIME" => "'00:00:00'",
                "INTERVAL" => "'0'",
                _ => "0" // integer/bigint/smallint/decimal/numeric/double precision/real and fallback
            };
        }
    }
}
