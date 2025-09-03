using System.Collections.Generic;

namespace nORM.Migration
{
    public interface IMigrationSqlGenerator
    {
        MigrationSqlStatements GenerateSql(SchemaDiff diff);
    }

    public record MigrationSqlStatements(IReadOnlyList<string> Up, IReadOnlyList<string> Down);
}
