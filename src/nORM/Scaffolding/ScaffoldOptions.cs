#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    /// <summary>
    /// Options that control database scaffolding output.
    /// </summary>
    public sealed class ScaffoldOptions
    {
        /// <summary>
        /// Gets the optional table filter. Entries may be bare table names or
        /// schema-qualified names such as <c>dbo.Customer</c>. When empty, all
        /// discovered user tables are scaffolded.
        /// </summary>
        public IReadOnlyCollection<string> Tables { get; init; } = Array.Empty<string>();

        /// <summary>
        /// Gets a value indicating whether existing generated files may be
        /// overwritten. The default preserves the historical scaffolder behavior.
        /// </summary>
        public bool OverwriteFiles { get; init; } = true;
    }
}
